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

let client = null
let room = null
let myName = null
let myClientId = null
let entered = false
let sysLog = []
let messages = []
let online = new Map()
let lastSeen = new Map()
let heartbeatTimer = null
let cleanupTimer = null

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

function startChat() {
  const nameErr = validateName(usernameInput.value)
  if (nameErr) { showLoginError(nameErr); return }

  room = sanitizeRoom(roomInput.value)
  myName = usernameInput.value.trim()
  myClientId = uid()
  entered = false

  loginScreen.classList.add('hidden')
  chatScreen.classList.remove('hidden')
  roomTitle.textContent = room
  messageInput.focus()
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
}

function onConnect() {
  if (!client) return
  client.subscribe(topicFor('chat'))
  client.subscribe(topicFor('presence') + '/+')
  announce()
  heartbeatTimer = setInterval(announce, HEARTBEAT_MS)
  cleanupTimer = setInterval(cleanupStale, 5000)
  setStatus('已连接 · 群聊中', true)
  setTimeout(checkNameConflict, 3000)
  renderChat()
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

function onMessage(topic, payload) {
  const str = payload.toString()
  if (topic.indexOf('/presence/') !== -1) {
    handlePresence(topic, str)
    return
  }
  if (topic === topicFor('chat')) {
    try {
      const m = JSON.parse(str)
      if (m && m.id) { messages.push(m); renderChat() }
    } catch (_) {}
  }
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
    li.appendChild(dot)
    li.appendChild(label)
    frag.appendChild(li)
  }
  membersList.appendChild(frag)
  memberCountEl.textContent = String(list.length)
}

function pushSys(text) {
  sysLog.push({ sys: true, text, t: Date.now() })
  sysLog = sysLog.slice(-30)
  renderChat()
}

function sendMessage() {
  const text = messageInput.value.trim()
  if (!text || !client || !entered) return
  publishMessage({ text })
  messageInput.value = ''
  messageInput.focus()
}

function publishMessage(extra) {
  const m = Object.assign({
    id: uid(), sender: myName, text: '', t: Date.now()
  }, extra)
  messages.push(m)
  client.publish(topicFor('chat'), JSON.stringify(m), { qos: 0 })
  renderChat()
}

function sendImage(file) {
  if (!file || !client || !entered) return
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
  if (m.img) {
    const a = document.createElement('a')
    a.href = m.img
    a.target = '_blank'
    a.rel = 'noopener'
    const img = document.createElement('img')
    img.className = 'bubble-img'
    img.src = m.img
    img.alt = '图片'
    a.appendChild(img)
    wrap.appendChild(a)
  } else {
    const bubble = document.createElement('div')
    bubble.className = 'bubble'
    bubble.textContent = m.text
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
  sysLog = []
  messages = []
  online = new Map()
  lastSeen = new Map()
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
  if (e.key === 'Enter') { e.preventDefault(); sendMessage() }
})
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
