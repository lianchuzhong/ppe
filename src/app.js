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
const passwordInput = $('password')
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
let cryptoKey = null
let decryptedCache = new Map()
let decrypting = new Set()
let heartbeatTimer = null
let cleanupTimer = null

function uid() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 10)
}

function timeStr(t) {
  return new Date(t).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })
}

function b64(buf) {
  let bin = ''
  for (const b of buf) bin += String.fromCharCode(b)
  return btoa(bin)
}

function unb64(s) {
  const bin = atob(s)
  const out = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
  return out
}

async function deriveKey(room, password) {
  const pass = (password || '').trim()
  if (!pass) return null
  const enc = new TextEncoder()
  const base = await crypto.subtle.importKey('raw', enc.encode(pass), 'PBKDF2', false, ['deriveKey'])
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt: enc.encode('succession-chat/' + room), iterations: 120000, hash: 'SHA-256' },
    base,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt']
  )
}

async function encryptData(obj) {
  if (!cryptoKey) return null
  const iv = crypto.getRandomValues(new Uint8Array(12))
  const buf = new TextEncoder().encode(JSON.stringify(obj))
  const ct = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, cryptoKey, buf)
  return { iv: b64(iv), data: b64(new Uint8Array(ct)) }
}

async function decryptData(m) {
  try {
    const iv = unb64(m.iv)
    const ct = unb64(m.data)
    const pt = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, cryptoKey, ct)
    return JSON.parse(new TextDecoder().decode(pt))
  } catch (_) { return null }
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

function randInt(max) {
  const arr = new Uint32Array(1)
  crypto.getRandomValues(arr)
  return arr[0] % max
}

function randomPassphrase() {
  const w = []
  for (let i = 0; i < 4; i++) w.push(WORDS[randInt(WORDS.length)])
  return w.join('-') + '-' + (100 + randInt(900))
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

  deriveKey(room, passwordInput.value).then((key) => {
    cryptoKey = key
    if (key) setStatus('正在连接…… · 消息已加密', false)
    else setStatus('正在连接……（未加密）', false)
  })

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

async function sendMessage() {
  const text = messageInput.value.trim()
  if (!text || !client || !entered) return
  await publishMessage({ text })
  messageInput.value = ''
  messageInput.focus()
}

async function publishMessage(extra) {
  const base = { id: uid(), sender: myName, t: Date.now() }
  let wire
  if (cryptoKey) {
    const enc = await encryptData(extra)
    if (!enc) return
    wire = Object.assign(base, { iv: enc.iv, data: enc.data })
  } else {
    wire = Object.assign(base, extra)
  }
  messages.push(wire)
  if (cryptoKey) decryptedCache.set(wire.id, extra)
  client.publish(topicFor('chat'), JSON.stringify(wire), { qos: 0 })
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

function messageContent(m) {
  if (m.text != null) return { text: m.text }
  if (m.data) {
    if (decryptedCache.has(m.id)) {
      const d = decryptedCache.get(m.id)
      return d ? d : { failed: true }
    }
    decryptAsync(m)
    return { pending: true }
  }
  return { text: '' }
}

async function decryptAsync(m) {
  if (decrypting.has(m.id)) return
  decrypting.add(m.id)
  let d = null
  if (cryptoKey) d = await decryptData(m)
  decryptedCache.set(m.id, d)
  decrypting.delete(m.id)
  renderChat()
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
    const a = document.createElement('a')
    a.href = c.img
    a.target = '_blank'
    a.rel = 'noopener'
    const img = document.createElement('img')
    img.className = 'bubble-img'
    img.src = c.img
    img.alt = '图片'
    a.appendChild(img)
    wrap.appendChild(a)
  } else {
    const bubble = document.createElement('div')
    bubble.className = 'bubble'
    if (c.failed) bubble.textContent = '[加密消息 · 密码不匹配，无法解密]'
    else if (c.pending) bubble.textContent = '正在解密…'
    else bubble.textContent = c.text
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
  cryptoKey = null
  decryptedCache = new Map()
  decrypting = new Set()
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
$('gen-btn').addEventListener('click', () => {
  const p = randomPassphrase()
  passwordInput.value = p
  try { navigator.clipboard.writeText(p) } catch (_) {}
  toast('已生成随机口令并复制，请分享给群成员')
})
$('show-btn').addEventListener('click', () => {
  const shown = passwordInput.type === 'text'
  passwordInput.type = shown ? 'password' : 'text'
  $('show-btn').textContent = shown ? '👁' : '🙈'
})
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
