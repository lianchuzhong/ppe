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
const mentionBox = $('mention-box')

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

function uid() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 10)
}

function timeStr(t) {
  return new Date(t).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })
}

const WORDS = ('ahead amber apple atlas bacon badge baker bamboo beacon bear berry birch bliss bloom ' +
  'blue bolt bonus book brave breeze brick bright bronze brush cable cabin cactus camel candy canoe ' +
  'canyon cargo cedar charm cherry chess chunk civic cloud clover cobra cocoa comet comic coral cosmic ' +
  'cotton cricket crystal curve cycle daisy dancer dawn delta denim desert diamond diver dolphin dragon ' +
  'drift drum eagle ember emerald engine feather fern field finch flame flint flock flower foam forest ' +
  'fossil fox frost galaxy garden gem ginger glacier glow gold gopher grape gravel guitar harbor hazel ' +
  'helix heron honey horizon ibex icicle igloo indigo island ivory jade jaguar jasmine jetty jigsaw ' +
  'jungle kayak koala lagoon lantern laurel lemon leopard lilac lime lion lotus lynx magnet maple marble ' +
  'meadow melon meteor mint mirror mist moon moose moss moth mountain navy nebula nickel night ocean ' +
  'olive onion opal orange orchid otter oyster paddle palace palm panel paper parrot pearl pepper piano ' +
  'pika pilot pine pirate planet plaza plum pond pony poppy prism puma quartz rabbit raven reed reef ' +
  'ribbon ridge river robin rocket rose ruby sail salmon sand sapphire scarf shadow shell shine silver ' +
  'skunk slate smoke snow sock spark sparrow spice spider spring squash squirrel stone storm sugar ' +
  'sunrise sunset swift tangerine teal tiger timber toast tomato topaz torch trail tulip turtle valley ' +
  'velvet violet walnut wander water willow wind winter wolf zebra zephyr').split(' ')

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
  client.subscribe(topicFor('reviewed'))
  client.subscribe(topicFor('rejected'))
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
  if (topic === topicFor('rejected')) {
    try {
      const m = JSON.parse(str)
      if (m && m.id && m.sender === myName && seenIds.has(m.id)) {
        const idx = messages.findIndex((x) => x.id === m.id)
        if (idx !== -1) {
          messages.splice(idx, 1)
          seenIds.delete(m.id)
          pushSys('你的消息已被管理员拒绝')
        }
      }
    } catch (_) {}
    return
  }
  if (topic === topicFor('reviewed')) {
    try {
      const m = JSON.parse(str)
      if (m && m.id) {
        if (seenIds.has(m.id)) {
          const local = messages.find((x) => x.id === m.id)
          if (local && local.awaitingReview) {
            delete local.awaitingReview
            renderChat()
          }
        } else {
          seenIds.add(m.id)
          messages.push(m)
          renderChat()
        }
      }
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
  await publishMessage({ text })
  messageInput.value = ''
  messageInput.focus()
}

async function publishMessage(extra) {
  const base = { id: uid(), sender: myName, t: Date.now() }
  const wire = Object.assign(base, extra)
  messages.push(wire)
  seenIds.add(wire.id)
  wire.awaitingReview = true
  client.publish(topicFor('chat'), JSON.stringify(wire), { qos: 0 })
  renderChat()
  
  fetch('/api/notify', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sender: myName, text: extra.text || '', room: room, t: base.t }),
  }).catch(() => {})
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

function messageContent(m) {
  if (m.img != null) return { img: m.img }
  if (m.text != null) return { text: m.text }
  return { text: '' }
}

function updateMentionBox() {
  if (!mentionBox) return
  const val = messageInput.value
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
  meta.textContent = `${m.sender} · ${timeStr(m.t)}${m.awaitingReview ? ' · 待审核' : ''}`
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
