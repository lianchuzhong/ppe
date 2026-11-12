import mqtt from 'mqtt'

const BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt',
]
const ROOM_PREFIX = 'succession/'
const MAX_RENDER = 200
const HEARTBEAT_MS = 8000
const STALE_MS = 25000
const IMG_MAX_DIM = 900
const IMG_MAX_BYTES = 500 * 1024

// 房间开关：
// - 所有房间默认「未开启」，只有收到控制消息才开启（管理员/付费自动）
// - 控制消息走 retained 发布到 succession/<房间号>/control，后来者一进房间就能知道状态
// - CONTROL_KEY 与房间管理网关（room-gateway.js）保持一致，防止路人误发布；公开页面防误触不防恶意
const CONTROL_KEY = 'lcz-room-official-9f3a2c'
// 房间状态等待窗口：进房后这段时间内没有收到控制消息，就按「未开启」处理
const STATE_SETTLE_MS = 3200

// PayPal 订阅（房间开启权，$2/月）
const PLAN_ID = 'P-1WB30980E1784674TNLWJKXI'
const PAYPAL_SDK = 'https://www.paypal.com/sdk/js?client-id=BAAJrPauJN8Rzb18zdhSHNn80M-wBAW6bLO_J5lu0IoM6utug8J6jvSZOxXtpaoXukDhaR3UY0aCBjIL0M&vault=true&intent=subscription'
const PAYPAL_FALLBACK = 'https://www.paypal.com/webapps/billing/plans/subscribe?plan_id=P-1WB30980E1784674TNLWJKXI'
// 多网站支持者校验（同一订阅在短视频站也有效）
const SUP_STATUS_API = 'https://lcz-video-alerts.lcz-video-alerts.workers.dev'
const LS_PAID = 'lcz_pperoom_paid'      // {subId, room, name, t}
const LS_REQUESTED = 'lcz_pperoom_req'  // room -> 提出过申请的标记
// 访客 beacon：浏览器会话随机 id，30 分钟内刷新视为同一访问（网关端也按它去重）
const LS_VISIT_CID = 'lcz_pperoom_visit'
const VISIT_CID_TTL = 30 * 60 * 1000

const $ = (id) => document.getElementById(id)

const loginScreen = $('login-screen')
const chatScreen = $('chat-screen')
const loginError = $('login-error')
const usernameInput = $('username')
const roomInput = $('room')
const joinBtn = $('join-btn')
const membersList = $('members-list')
const memberCountEl = $('member-count')
const roomTitle = $('room-title')
const messagesBox = $('messages')
const messageInput = $('message-input')
const sendBtn = $('send-btn')
const imgBtn = $('img-btn')
const imgInput = $('img-input')
const leaveBtn = $('leave-btn')
const statusText = $('status-text')
const mentionBox = $('mention-box')

// 房间开启门相关的 DOM
const gateBox = $('room-gate')
const gateState = $('gate-state')
const gateTitle = $('gate-title')
const gateLead = $('gate-lead')
const gatePaypal = $('gate-paypal')
const gatePaylink = $('gate-paylink')
const gateRestore = $('gate-restore')
const gateDivider = $('gate-divider')
const requestArea = $('request-block')
const requestText = $('request-text')
const requestBtn = $('request-btn')
const requestSentNote = $('request-sent')
const requestAgain = $('request-again')
// 凭订阅号恢复（跨设备 / 在短视频等其他网站订阅的都可恢复）
const gateSublink = $('gate-sublink')
const gateSubrow = $('gate-subrow')
const gateSubinput = $('gate-subinput')
const gateSubbtn = $('gate-subbtn')

let client = null
let room = null
let myName = null
let myClientId = null
let entered = false
let sysLog = []
let messages = []
let online = new Map()
let lastSeen = new Map()
let seenIds = new Set()
let mentionState = null
let mentionItems = []
let mentionSel = -1
let heartbeatTimer = null
let cleanupTimer = null

// 房间开关状态
let roomOpen = false        // 房间是否开启；收到 control 消息才会变 true
let hasControl = false      // 是否收到过（保留）控制消息
let settleTimer = null      // 状态等待定时器
let paySdkTried = false
let paySdkTimer = null
let requesting = false

function uid() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 10)
}

function timeStr(t) {
  return new Date(t).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })
}

function topicFor(kind) {
  return ROOM_PREFIX + room + '/' + kind
}

function presenceTopic(id) {
  return topicFor('presence') + '/' + id
}

function sanitizeRoom(raw) {
  return (raw || '').trim().toLowerCase().replace(/[^a-z0-9_.-]/g, '').slice(0, 40) || 'global'
}

function validateName(raw) {
  const name = (raw || '').trim()
  if (!name) return '请填写昵称'
  if (name.length > 20) return '昵称不能超过 20 个字符'
  if (name === '系统') return '该昵称不可用，请换一个'
  return null
}

function showLoginError(msg) {
  loginError.textContent = msg
  loginError.classList.remove('hidden')
}

function setStatus(text, ok) {
  statusText.textContent = text
  statusText.className = 'status ' + (ok ? 'ok' : '')
}

function paidRecord() {
  try { return JSON.parse(localStorage.getItem(LS_PAID) || 'null') || null } catch (_) { return null }
}

function requestedFlag() {
  try { return localStorage.getItem(LS_REQUESTED + '_' + room) === '1' } catch (_) { return false }
}

function markRequested() {
  try { localStorage.setItem(LS_REQUESTED + '_' + room, '1') } catch (_) {}
}

/* ------------------------------------------------------------ 访客 beacon */

// 会话 id：localStorage 保存 30 分钟；过期就换新号（"同一访客"由网关按 cid+房间 去重）
function ensureVisitCid() {
  let rec = null
  try { rec = JSON.parse(localStorage.getItem(LS_VISIT_CID) || 'null') } catch (_) { rec = null }
  if (!rec || !rec.cid || !rec.t || Date.now() - rec.t > VISIT_CID_TTL) {
    rec = { cid: uid() + uid().slice(0, 4), t: Date.now() }
  } else {
    rec.t = Date.now()
  }
  try { localStorage.setItem(LS_VISIT_CID, JSON.stringify(rec)) } catch (_) {}
  return rec.cid
}

// 进房 beacon：分享聊天室的 MQTT 连接，非 retained（网关实时计数用）
function publishRoomVisit() {
  if (!client) return
  try {
    client.publish(ROOM_PREFIX + room + '/visit',
      JSON.stringify({ v: 1, cid: ensureVisitCid(), t: Date.now() }), { qos: 1, retain: false })
  } catch (_) {}
}

// 主页 beacon：登录页没有聊天连接，开一条一次性短连发布后立即关闭
function noteHomeVisit() {
  const cid = ensureVisitCid()
  const c = mqtt.connect(BROKERS[0], {
    clientId: 'vst-' + uid(),
    reconnectPeriod: 0,      // 不重试：beacon 是尽力而为，失败不值得加重连负担
    connectTimeout: 8000,
    keepalive: 15,
  })
  let done = false
  const finish = () => { if (done) return; done = true; try { c.end(true) } catch (_) {} }
  c.on('connect', () => {
    try {
      c.publish(ROOM_PREFIX + '_meta/visit',
        JSON.stringify({ v: 1, cid, t: Date.now() }), { qos: 1, retain: false })
    } catch (_) {}
    setTimeout(finish, 600)
  })
  c.on('error', finish)
  c.on('close', finish)
}

/* ------------------------------------------------------------ 房间开启门 */

function updateGate() {
  if (!gateBox) return
  if (roomOpen) {
    gateBox.classList.add('hidden')
    inputEnabled(true)
    return
  }
  gateBox.classList.remove('hidden')
  inputEnabled(false)
  if (hasControl) {
    gateState.textContent = ''
    gateTitle.textContent = '该房间未开启'
    gateLead.textContent = '本房间尚未开启。付费订阅后会自动开启，或提交申请由科学家在后台开启。'
    prepareGateActions()
  } else {
    gateState.textContent = '正在检查房间状态……'
    gateTitle.textContent = '连接房间中'
    gateLead.textContent = '正在确认这个房间号是否已开启，请稍候。'
  }
}

function inputEnabled(enabled) {
  if (messageInput) {
    messageInput.disabled = !enabled
    messageInput.placeholder = enabled ? '输入消息，回车发送' : '房间未开启，暂时不能发言'
  }
  if (sendBtn) sendBtn.disabled = !enabled
  if (imgBtn) imgBtn.disabled = !enabled
}

// 未开启时的行动区：已付费恢复 / PayPal 按钮 / 申请表
function prepareGateActions() {
  const rec = paidRecord()
  if (gateRestore) {
    if (rec && rec.subId) {
      gateRestore.classList.remove('hidden')
      gateRestore.textContent = '我已付费订阅（' + subIdTail(rec.subId) + '）· 恢复本房间的开启'
    } else {
      gateRestore.classList.add('hidden')
    }
  }
  if (gateSublink) gateSublink.classList.remove('hidden')
  if (gatePaylink) gatePaylink.classList.remove('hidden')
  const sent = requestedFlag() || requesting
  updateRequestArea(sent)
  if (!sent) ensurePaypal()
}

function subIdTail(s) {
  return String(s || '').slice(-6) || '——'
}

// 凭订阅号恢复：换设备 / 在短视频站订阅过的，输入 I-xxxxx 校验通过即开房
async function restoreBySubId() {
  const v = String(gateSubinput ? gateSubinput.value : '').trim()
  if (!v) { toast('请输入 PayPal 订阅号'); return }
  if (!/^I-[A-Za-z0-9-]{6,40}$/.test(v)) { toast('订阅号形如 I-xxxxxxxx，请到 PayPal 订阅记录里复制'); return }
  if (gateSubbtn) gateSubbtn.disabled = true
  toast('校验订阅中……')
  try {
    const r = await fetch(SUP_STATUS_API + '/api/support/status?sub=' + encodeURIComponent(v), { credentials: 'omit' })
    const j = await r.json().catch(() => ({}))
    if (r.ok && j.ok && j.active) {
      try { localStorage.setItem(LS_PAID, JSON.stringify({ subId: v, room, name: myName, t: Date.now() })) } catch (_) {}
      publishOpen('paid', { subId: v })
      if (gateSubrow) gateSubrow.classList.add('hidden')
      toast('订阅有效 · 房间已开启')
    } else if (r.ok && j.ok) {
      toast('这个订阅号不在有效订阅中（' + (j.status || '已失效') + '）')
    } else {
      toast('订阅校验服务器还没接通（' + (j.error || 'unknown') + '），稍后再试')
    }
  } catch (_) {
    toast('订阅校验服务器不可达，稍后再试')
  }
  if (gateSubbtn) gateSubbtn.disabled = false
}

function updateRequestArea(sent) {
  if (requestArea) requestArea.classList.toggle('hidden', !!sent)
  if (requestSentNote) requestSentNote.classList.toggle('hidden', !sent)
  if (gateDivider) gateDivider.classList.toggle('hidden', !!sent)
}

function ensurePaypal() {
  if (!gatePaypal) return
  if (window.paypal && window.paypal.Buttons) { renderPayButtons(); return }
  if (paySdkTried) {
    // SDK 已试过但没就绪（被拦/超时）→ 纯链接兜底
    gatePaypal.classList.add('hidden')
    if (gatePaylink) gatePaylink.classList.add('show')
    return
  }
  paySdkTried = true
  const sc = document.createElement('script')
  sc.src = PAYPAL_SDK
  sc.onload = () => {
    if (window.paypal && window.paypal.Buttons) renderPayButtons()
    else payFallback()
  }
  sc.onerror = () => payFallback()
  document.head.appendChild(sc)
  // SDK 慢/被拦兜底：12 秒还没就绪就走纯链接
  paySdkTimer = setTimeout(() => {
    if (!window.paypal || !gatePaypal.dataset.rendered) payFallback()
  }, 12000)
}

function payFallback() {
  if (paySdkTimer) { clearTimeout(paySdkTimer); paySdkTimer = null }
  if (gatePaypal) gatePaypal.classList.add('hidden')
  if (gatePaylink) {
    gatePaylink.classList.add('show')
    gatePaylink.href = PAYPAL_FALLBACK
  }
}

function renderPayButtons() {
  if (!gatePaypal || gatePaypal.dataset.rendered) return
  try {
    gatePaypal.dataset.rendered = '1'
    window.paypal.Buttons({
      style: { shape: 'rect', color: 'gold', layout: 'vertical', label: 'subscribe' },
      createSubscription: (data, actions) => actions.subscription.create({ plan_id: PLAN_ID }),
      onApprove: (data) => {
        const subId = (data && data.subscriptionID) || ''
        try { localStorage.setItem(LS_PAID, JSON.stringify({ subId, room, name: myName, t: Date.now() })) } catch (_) {}
        publishOpen('paid', { subId })
        toast(subId ? '订阅成功 · 房间已开启' : '订阅完成 · 房间已开启')
        if (paySdkTimer) { clearTimeout(paySdkTimer); paySdkTimer = null }
      }
    }).render(gatePaypal)
  } catch (_) {
    delete gatePaypal.dataset.rendered
    payFallback()
  }
}

// 付费用户恢复：车库被关后一键再开（用自己的订阅记录）
function restorePaidRoom() {
  const rec = paidRecord()
  if (!rec || !rec.subId) { toast('没有找到付费记录，请先订阅'); return }
  publishOpen('paid', { subId: rec.subId })
  toast('已发布开启，稍候即可发言')
}

// 提交开启申请（免费通道）：发到 request 主题（retained），管理网关收到后创建 GitHub Issue
function submitRequest() {
  if (!client || !myName) return
  if (requesting) return
  requesting = true
  markRequested()
  const rid = uid()
  const payload = {
    v: 1,
    id: rid,
    room,
    name: myName,
    text: (requestText ? String(requestText.value || '').slice(0, 500) : ''),
    t: Date.now(),
  }
  try {
    client.publish(topicFor('request'), JSON.stringify(payload), { qos: 1, retain: true })
  } catch (_) {}
  updateRequestArea(true)
  toast('申请已提交，等待科学家开启')
  gateState.textContent = ''
}

/* ------------------------------------------------------------ 聊天 */

function startChat() {
  const nameErr = validateName(usernameInput.value)
  if (nameErr) { showLoginError(nameErr); return }

  room = sanitizeRoom(roomInput.value)
  myName = usernameInput.value.trim()
  myClientId = uid()
  entered = false

  roomOpen = false
  hasControl = false
  requesting = false
  paySdkTried = false
  if (gatePaypal) { delete gatePaypal.dataset.rendered; gatePaypal.textContent = '' }
  if (gatePaylink) gatePaylink.classList.remove('show')
  if (requestText) requestText.value = ''

  loginScreen.classList.add('hidden')
  chatScreen.classList.remove('hidden')
  roomTitle.textContent = room
  updateGate()
  setStatus('正在连接……', false)

  client = mqtt.connect(BROKERS[0], {
    clientId: 'succ-' + myClientId,
    reconnectPeriod: 2000,
    connectTimeout: 15000,
    will: {
      topic: presenceTopic(myClientId),
      payload: JSON.stringify({ name: myName, online: false, t: Date.now() }),
      qos: 1,
      retain: true,
    },
  })

  client.on('connect', onConnect)
  client.on('reconnect', () => setStatus('重连中……', false))
  client.on('close', () => { if (entered) setStatus('连接断开，正在重连', false) })
  client.on('error', () => {})
  client.on('message', onMessage)

  // 等待窗口内没收到任何控制消息 → 状态已确认：该房间确实未开启（所有房间默认关闭）
  if (settleTimer) clearTimeout(settleTimer)
  settleTimer = setTimeout(() => {
    if (!client) return
    if (!hasControl) {
      hasControl = true
      roomOpen = false
      updateGate()
      pushSys('房间状态：未开启')
    }
  }, STATE_SETTLE_MS)
}

function onConnect() {
  if (!client) return
  client.subscribe(topicFor('control'))
  client.subscribe(topicFor('chat'))
  client.subscribe(topicFor('presence') + '/+')
  announce()
  publishRoomVisit()   // 进房访客记录（30 分钟会话去重在网关端）
  heartbeatTimer = setInterval(announce, HEARTBEAT_MS)
  cleanupTimer = setInterval(cleanupStale, 5000)
  setStatus('已连接 · ' + (roomOpen ? '群聊中' : '房间待开启'), true)
  setTimeout(checkNameConflict, 3000)
}

function publishOpen(by, extra) {
  if (!client) return
  const payload = Object.assign({
    v: 1,
    open: true,
    by,               // paid = 付费自动 / admin = 管理员
    name: myName,
    room,
    k: CONTROL_KEY,   // 防路人误发布（公开页面，防误触不防恶意）
    t: Date.now(),
  }, extra || {})
  try {
    client.publish(topicFor('control'), JSON.stringify(payload), { qos: 1, retain: true })
  } catch (_) {}
}

function onMessage(topic, payload) {
  const str = payload.toString()
  if (!str) return
  if (topic.indexOf('/presence/') !== -1) {
    handlePresence(topic, str)
    return
  }
  if (topic === topicFor('control')) {
    handleControl(str)
    return
  }
  if (topic === topicFor('chat')) {
    try {
      const m = JSON.parse(str)
      if (!m || !m.id || seenIds.has(m.id)) return
      seenIds.add(m.id)
      messages.push(m)
      messages = messages.slice(-MAX_RENDER * 2)
      renderChat()
    } catch (_) {}
  }
}

function handleControl(str) {
  let c
  try { c = JSON.parse(str) } catch (_) { return }
  if (!c || typeof c.open !== 'boolean') return
  if (c.k !== CONTROL_KEY) return
  const wasOpen = roomOpen
  hasControl = true
  roomOpen = !!c.open
  updateGate()
  if (roomOpen && !wasOpen) {
    pushSys(c.by === 'paid' ? '房间已开启（本房间由付费订阅自动开启）' : '房间已开启，可以发言了')
  } else if (!roomOpen && wasOpen) {
    pushSys('房间已被关闭。将继续收不到新消息，可重新付费或提交申请。')
  }
  if (hasControl && settleTimer) { clearTimeout(settleTimer); settleTimer = null }
}

function announce() {
  if (!client) return
  client.publish(presenceTopic(myClientId), JSON.stringify({
    name: myName, online: true, t: Date.now()
  }), { qos: 1, retain: true })
  lastSeen.set(myClientId, Date.now())
}

function checkNameConflict() {
  if (!entered && myName && onlineHas(myName)) {
    failEnter('该昵称已被占用，请换一个名字')
    return
  }
  entered = true
}

function onlineHas(name) {
  for (const [id, n] of online) {
    if (id !== myClientId && n === name) return true
  }
  return false
}

function handlePresence(topic, str) {
  let p
  try { p = JSON.parse(str) } catch (_) { return }
  if (!p || typeof p.name !== 'string') return
  const id = topic.slice(topic.lastIndexOf('/') + 1)
  lastSeen.set(id, Date.now())
  if (p.online) {
    if (!online.has(id) && id !== myClientId) pushSys(`${p.name} 加入了群聊`)
    online.set(id, p.name)
  } else {
    const gone = online.get(id)
    if (gone && id !== myClientId) pushSys(`${gone} 离开了群聊`)
    online.delete(id)
  }
  updateOnline()
}

function cleanupStale() {
  const now = Date.now()
  for (const [id, seen] of lastSeen) {
    if (id !== myClientId && now - seen > STALE_MS && online.has(id)) {
      const n = online.get(id)
      online.delete(id)
      pushSys(`${n} 离开了群聊`)
    }
  }
  updateOnline()
}

function updateOnline() {
  const list = [...online.values()]
  membersList.textContent = ''
  const frag = document.createDocumentFragment()
  for (const n of list) {
    const li = document.createElement('div')
    li.className = 'member' + (n === myName ? ' me' : '')
    const dot = document.createElement('span')
    dot.className = 'dot'
    const label = document.createElement('span')
    label.className = 'mname'
    label.textContent = n + (n === myName ? '（我）' : '')
    if (n !== myName) {
      label.title = '点击 @' + n
      label.addEventListener('click', () => appendMention(n))
    }
    li.appendChild(dot)
    li.appendChild(label)
    frag.appendChild(li)
  }
  membersList.appendChild(frag)
  memberCountEl.textContent = String(list.length)
  updateMentionBox()
}

function pushSys(text) {
  sysLog.push({ sys: true, text, t: Date.now() })
  sysLog = sysLog.slice(-30)
  renderChat()
}

async function sendMessage() {
  const text = messageInput.value.trim()
  if (!text || !client || !entered) return
  if (!roomOpen) { toast('房间未开启，请先订阅或提交申请'); return }
  await publishMessage({ text })
  messageInput.value = ''
  messageInput.focus()
}

async function publishMessage(extra) {
  const base = { id: uid(), sender: myName, t: Date.now() }
  const wire = Object.assign(base, extra)
  messages.push(wire)
  seenIds.add(wire.id)
  messages = messages.slice(-MAX_RENDER * 2)
  client.publish(topicFor('chat'), JSON.stringify(wire), { qos: 0 })
  renderChat()
}

function sendImage(file) {
  if (!file || !client || !entered) return
  if (!roomOpen) { toast('房间未开启，请先订阅或提交申请'); return }
  if (!/^image\//.test(file.type)) { toast('请选择图片文件'); return }
  const reader = new FileReader()
  reader.onload = () => {
    compressImage(reader.result).then((dataUrl) => {
      if (dataUrl) publishMessage({ img: dataUrl })
    })
  }
  reader.readAsDataURL(file)
}

function compressImage(dataUrl) {
  return new Promise((resolve) => {
    const img = new Image()
    img.onload = () => {
      let { width, height } = img
      const scale = Math.min(1, IMG_MAX_DIM / Math.max(width, height))
      width = Math.round(width * scale)
      height = Math.round(height * scale)
      const canvas = document.createElement('canvas')
      canvas.width = width
      canvas.height = height
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#fff'
      ctx.fillRect(0, 0, width, height)
      ctx.drawImage(img, 0, 0, width, height)
      let quality = 0.82
      let out = canvas.toDataURL('image/jpeg', quality)
      while (out.length > IMG_MAX_BYTES && quality > 0.3) {
        quality -= 0.08
        out = canvas.toDataURL('image/jpeg', quality)
      }
      resolve(out)
    }
    img.onerror = () => resolve(null)
    img.src = dataUrl
  })
}

function renderChat() {
  if (!messagesBox) return
  const box = messagesBox
  const wasAtBottom = box.scrollTop + box.clientHeight >= box.scrollHeight - 24
  const items = []
  for (const m of messages) items.push(m)
  for (const s of sysLog) items.push(s)
  items.sort((a, b) => (a.t || 0) - (b.t || 0))
  const slice = items.slice(-MAX_RENDER)
  box.textContent = ''
  const frag = document.createDocumentFragment()
  for (const m of slice) frag.appendChild(renderMessageNode(m))
  box.appendChild(frag)
  if (wasAtBottom) box.scrollTop = box.scrollHeight
}

function messageContent(m) {
  if (m.img != null) return { img: m.img }
  if (m.text != null) return { text: m.text }
  return { text: '' }
}

function updateMentionBox() {
  if (!mentionBox) return
  const val = messageInput.value
  if (!val) { hideMention(); return }
  const caret = messageInput.selectionStart == null ? val.length : messageInput.selectionStart
  const at = val.lastIndexOf('@', caret - 1)
  if (at === -1) { hideMention(); return }
  const word = val.slice(at + 1, caret)
  if (/[\s@]/.test(word)) { hideMention(); return }
  const q = word.toLowerCase()
  const names = [...online.values()].filter((n) => n !== myName && n.toLowerCase().includes(q))
  if (names.length === 0) { hideMention(); return }
  mentionState = { at, word }
  mentionItems = names
  mentionSel = 0
  mentionBox.textContent = ''
  const frag = document.createDocumentFragment()
  for (let i = 0; i < names.length; i++) {
    const item = document.createElement('div')
    item.className = 'mitem' + (i === 0 ? ' sel' : '')
    item.textContent = '@' + names[i]
    item.addEventListener('click', () => insertMention(names[i]))
    frag.appendChild(item)
  }
  mentionBox.appendChild(frag)
  mentionBox.classList.remove('hidden')
}

function hideMention() {
  if (!mentionBox) return
  mentionBox.classList.add('hidden')
  mentionBox.textContent = ''
  mentionState = null
  mentionItems = []
  mentionSel = -1
}

function insertMention(name) {
  if (!mentionState) { appendMention(name); return }
  const val = messageInput.value
  const end = mentionState.at + 1 + mentionState.word.length
  messageInput.value = val.slice(0, mentionState.at) + '@' + name + ' ' + val.slice(end)
  hideMention()
  messageInput.focus()
  const pos = mentionState.at + name.length + 2
  messageInput.setSelectionRange(pos, pos)
}

function appendMention(name) {
  const val = messageInput.value
  const pos = messageInput.selectionStart == null ? val.length : messageInput.selectionStart
  const ins = '@' + name + ' '
  messageInput.value = val.slice(0, pos) + ins + val.slice(pos)
  hideMention()
  messageInput.focus()
  messageInput.setSelectionRange(pos + ins.length, pos + ins.length)
}

function moveMention(dir) {
  if (mentionItems.length === 0) return
  mentionSel = (mentionSel + dir + mentionItems.length) % mentionItems.length
  const items = mentionBox.querySelectorAll('.mitem')
  items.forEach((el, i) => el.classList.toggle('sel', i === mentionSel))
}

function renderTextWithMentions(text) {
  const frag = document.createDocumentFragment()
  const re = /@([^\s@]+)/g
  let last = 0
  let m
  while ((m = re.exec(text))) {
    if (m.index > last) frag.appendChild(document.createTextNode(text.slice(last, m.index)))
    const span = document.createElement('span')
    span.className = 'mention'
    span.textContent = m[0]
    frag.appendChild(span)
    last = m.index + m[0].length
  }
  if (last < text.length) frag.appendChild(document.createTextNode(text.slice(last)))
  return frag
}

function renderMessageNode(m) {
  const wrap = document.createElement('div')
  if (m.sys) {
    wrap.className = 'msg sys'
    const text = document.createElement('span')
    text.textContent = m.text
    wrap.appendChild(text)
    return wrap
  }
  const mine = m.sender === myName
  wrap.className = 'msg ' + (mine ? 'mine' : 'other')
  const meta = document.createElement('div')
  meta.className = 'meta'
  meta.textContent = `${m.sender} · ${timeStr(m.t)}`
  wrap.appendChild(meta)
  const c = messageContent(m)
  if (c.img) {
    const block = document.createElement('div')
    block.className = 'img-block'
    const a = document.createElement('a')
    a.href = c.img
    a.target = '_blank'
    a.rel = 'noopener'
    const img = document.createElement('img')
    img.className = 'bubble-img'
    img.src = c.img
    img.alt = '图片'
    a.appendChild(img)
    block.appendChild(a)
    const actions = document.createElement('div')
    actions.className = 'img-actions'
    const save = document.createElement('button')
    save.type = 'button'
    save.className = 'img-save'
    save.textContent = '保存图片'
    save.addEventListener('click', () => {
      const dl = document.createElement('a')
      dl.href = c.img
      dl.download = 'image-' + (m.id || Date.now()) + '.jpg'
      document.body.appendChild(dl)
      dl.click()
      dl.remove()
    })
    actions.appendChild(save)
    block.appendChild(actions)
    wrap.appendChild(block)
  } else {
    const bubble = document.createElement('div')
    bubble.className = 'bubble'
    bubble.appendChild(renderTextWithMentions(c.text))
    wrap.appendChild(bubble)
  }
  return wrap
}

function failEnter(msg) {
  showLoginError(msg)
  teardown()
  loginScreen.classList.remove('hidden')
  chatScreen.classList.add('hidden')
}

function teardown() {
  if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null }
  if (cleanupTimer) { clearInterval(cleanupTimer); cleanupTimer = null }
  if (settleTimer) { clearTimeout(settleTimer); settleTimer = null }
  if (paySdkTimer) { clearTimeout(paySdkTimer); paySdkTimer = null }
  if (client) {
    try {
      client.publish(presenceTopic(myClientId), JSON.stringify({
        name: myName, online: false, t: Date.now()
      }), { qos: 1, retain: true })
      client.end(true)
    } catch (_) {}
  }
  client = null
  room = null
  myName = null
  myClientId = null
  entered = false
  roomOpen = false
  hasControl = false
  requesting = false
  sysLog = []
  messages = []
  online = new Map()
  lastSeen = new Map()
  seenIds = new Set()
  hideMention()
  messageInput.value = ''
  messagesBox.textContent = ''
  membersList.textContent = ''
}

let toastTimer = null
function toast(msg) {
  const el = $('toast')
  if (!el) return
  el.textContent = msg
  el.classList.add('show')
  if (toastTimer) clearTimeout(toastTimer)
  toastTimer = setTimeout(() => el.classList.remove('show'), 2500)
}

joinBtn.addEventListener('click', startChat)
leaveBtn.addEventListener('click', () => {
  teardown()
  loginScreen.classList.remove('hidden')
  chatScreen.classList.add('hidden')
})
messageInput.addEventListener('keydown', (e) => {
  if (mentionState) {
    if (e.key === 'ArrowDown') { e.preventDefault(); moveMention(1); return }
    if (e.key === 'ArrowUp') { e.preventDefault(); moveMention(-1); return }
    if (e.key === 'Tab' || e.key === 'Enter') {
      e.preventDefault()
      if (mentionItems[mentionSel]) insertMention(mentionItems[mentionSel])
      return
    }
    if (e.key === 'Escape') { hideMention(); return }
  }
  if (e.key === 'Enter') { e.preventDefault(); sendMessage() }
})
messageInput.addEventListener('input', updateMentionBox)
sendBtn.addEventListener('click', sendMessage)
imgBtn.addEventListener('click', () => imgInput.click())
imgInput.addEventListener('change', () => {
  if (imgInput.files && imgInput.files[0]) sendImage(imgInput.files[0])
  imgInput.value = ''
})
for (const el of [usernameInput, roomInput]) {
  el.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); startChat() }
  })
}
if (gateRestore) gateRestore.addEventListener('click', restorePaidRoom)
if (requestBtn) requestBtn.addEventListener('click', submitRequest)
if (gateSublink) gateSublink.addEventListener('click', (e) => {
  e.preventDefault()
  if (gateSubrow) gateSubrow.classList.toggle('hidden')
})
if (gateSubbtn) gateSubbtn.addEventListener('click', restoreBySubId)
if (gateSubinput) gateSubinput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { e.preventDefault(); restoreBySubId() }
})
if (requestAgain) requestAgain.addEventListener('click', () => {
  // 撤回上一次申请（例如被拒绝/想改留言）：清标记重新填写
  try { localStorage.removeItem(LS_REQUESTED + '_' + room) } catch (_) {}
  requesting = false
  updateRequestArea(false)
})

// 主页访客记录：登录页所有人都会被打点（30 分钟同会话只在网关端计一次）
noteHomeVisit()
