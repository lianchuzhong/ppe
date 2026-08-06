import * as Y from 'yjs'
import { WebrtcProvider } from 'y-webrtc'

const SIGNALING = ['wss://signaling.yjs.dev']
const MAX_RENDER = 200
const STALE_MS = 20000
const RECLAIM_STALE_MS = 10000

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
const leaveBtn = $('leave-btn')
const statusText = $('status-text')

let doc = null
let provider = null
let messages = null
let names = null
let myName = null
let myClientId = null
let entered = false
let pendingStart = false
let cleanupTimer = null
let sysLog = []
let prevOnline = new Map()

function uid() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 10)
}

function timeStr(t) {
  return new Date(t).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })
}

function onlineIds() {
  return provider ? new Set(provider.awareness.getStates().keys()) : new Set()
}

function onlineNames() {
  const seen = new Map()
  if (!provider) return seen
  for (const [id, state] of provider.awareness.getStates()) {
    const n = state && state.user && state.user.name
    if (n) seen.set(id, n)
  }
  return seen
}

function ownName(name) {
  const v = names.get(name)
  return !!(v && v.id === myClientId)
}

function removeMyName() {
  if (names && myName && ownName(myName)) names.delete(myName)
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

function startChat() {
  const nameErr = validateName(usernameInput.value)
  if (nameErr) { showLoginError(nameErr); return }
  if (pendingStart) return
  pendingStart = true
  loginError.classList.add('hidden')

  const name = usernameInput.value.trim()
  const room = sanitizeRoom(roomInput.value)

  doc = new Y.Doc()
  provider = new WebrtcProvider(room, doc, { signaling: SIGNALING })
  messages = doc.getArray('messages')
  names = doc.getMap('names')
  myClientId = provider.awareness.clientID
  myName = name

  messages.observe(() => renderChat())
  provider.awareness.on('change', onAwarenessChange)
  provider.on('sync', (isSynced) => { if (isSynced) tryEnter() })
  setTimeout(tryEnter, 4000)

  statusText.textContent = '正在连接……'
  roomTitle.textContent = room
  loginScreen.classList.add('hidden')
  chatScreen.classList.remove('hidden')
  messageInput.focus()

  cleanupTimer = setInterval(cleanupStaleNames, 5000)
  renderChat()
}

function attemptClaim(name) {
  const current = names.get(name)
  if (current) {
    if (current.id === myClientId) return true
    const ownerOnline = onlineIds().has(current.id)
    const stale = Date.now() - current.t > RECLAIM_STALE_MS
    if (!ownerOnline && stale) {
      names.set(name, { id: myClientId, t: Date.now() })
      return true
    }
    return false
  }
  names.set(name, { id: myClientId, t: Date.now() })
  return true
}

function tryEnter() {
  if (!provider || entered) return
  if (attemptClaim(myName)) {
    entered = true
    pendingStart = false
    provider.awareness.setLocalState({ user: { name: myName } })
    prevOnline = onlineNames()
    statusText.textContent = '已连接 · 点对点加密'
    updateOnline()
  } else {
    failEnter('该昵称已被占用，请换一个名字')
  }
}

function failEnter(msg) {
  if (!pendingStart && !entered) return
  showLoginError(msg)
  teardown()
  loginScreen.classList.remove('hidden')
  chatScreen.classList.add('hidden')
}

function teardown() {
  if (cleanupTimer) { clearInterval(cleanupTimer); cleanupTimer = null }
  if (provider) {
    provider.awareness.setLocalState(null)
    removeMyName()
    try { provider.destroy() } catch (_) {}
  }
  if (doc) { try { doc.destroy() } catch (_) {} }
  provider = null
  doc = null
  messages = null
  names = null
  myName = null
  myClientId = null
  entered = false
  pendingStart = false
  sysLog = []
  prevOnline = new Map()
  messageInput.value = ''
  messagesBox.textContent = ''
  membersList.textContent = ''
}

function sendMessage() {
  const text = messageInput.value.trim()
  if (!text || !messages) return
  messages.push([{ id: uid(), sender: myName, text, t: Date.now() }])
  messageInput.value = ''
  messageInput.focus()
}

function pushSys(text) {
  sysLog.push({ sys: true, text, t: Date.now() })
  sysLog = sysLog.slice(-30)
  renderChat()
}

function onAwarenessChange(change) {
  if (!entered) return
  for (const id of change.added || []) {
    const st = provider.awareness.getStates().get(id)
    const n = st && st.user && st.user.name
    if (n && n !== myName) pushSys(`${n} 加入了群聊`)
  }
  for (const id of change.removed || []) {
    const n = prevOnline.get(id)
    if (n && n !== myName) pushSys(`${n} 离开了群聊`)
  }
  prevOnline = onlineNames()
  updateOnline()
}

function updateOnline() {
  if (!provider) return
  const namesMap = onlineNames()
  const list = [...namesMap.values()]
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

function renderChat() {
  if (!messages) return
  const box = messagesBox
  const wasAtBottom = box.scrollTop + box.clientHeight >= box.scrollHeight - 24
  const items = []
  for (const m of messages.toArray()) items.push(m)
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
  const bubble = document.createElement('div')
  bubble.className = 'bubble'
  bubble.textContent = m.text
  wrap.appendChild(meta)
  wrap.appendChild(bubble)
  return wrap
}

function cleanupStaleNames() {
  if (!names) return
  const now = Date.now()
  for (const [name, v] of names) {
    if (v.id === myClientId) continue
    if (!onlineIds().has(v.id) && now - v.t > STALE_MS) {
      names.delete(name)
    }
  }
}

setInterval(() => {
  if (entered && myName && names) {
    const cur = names.get(myName)
    if (!cur || cur.id !== myClientId) {
      failEnter('该昵称已被占用，请换一个名字')
    }
  }
}, 3000)

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
for (const el of [usernameInput, roomInput]) {
  el.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); startChat() }
  })
}
