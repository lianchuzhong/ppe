import mqtt from 'mqtt'
import http from 'node:http'
import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import os from 'node:os'
import { fileURLToPath } from 'node:url'

/* ================================================================
 * ppe 房间管理网关（替代旧 review-gateway，审核机制已取消）
 *
 * 职责：
 *  1. 订阅 succession/<房间>/request —— 收到「申请开启房间」后：
 *     - 记录到本机状态 rooms.json
 *     - 自动创建 GitHub Issue（通知科学家）
 *     - Issue / 控制台里给出一键开启链接（仅本机有效）
 *  2. 面板 http://127.0.0.1:8899 —— 列出待处理申请、已开启房间，
 *     点击按钮即可开启/关闭房间（发布 succession/<房间>/control 保留消息）
 *  3. 订阅 succession/<房间>/control —— 同步付费自动开房等状态
 *  4. 房间一开启，自动消化该房间的遗留申请（清 retained、关 Issue）
 *
 * 测试模式：环境变量 LCZ_ISSUES=off 时不创建真实 Issue。
 * ================================================================ */

const BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt',
]
const ROOM_PREFIX = 'succession/'
const PORT = 8899
const LOCAL_BASE = 'http://127.0.0.1:' + PORT
const GITHUB_REPO = 'lianchuzhong/ppe'
const GITHUB_API = 'https://api.github.com'

// 与页面 src/app.js 保持一致（防路人误发布，公开页面防误触不防恶意）
const CONTROL_KEY = 'lcz-room-official-9f3a2c'

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url))
const STATE_FILE = path.join(SCRIPT_DIR, 'rooms.json')
const PROCESSED_CAP = 400                                   // 记住最近 400 个已处理的申请 id，防 retained 重复处理
const REQUEST_TTL_MS = 14 * 24 * 3600 * 1000                // 超过 14 天的申请不再处理
const TEST_NO_ISSUES = String(process.env.LCZ_ISSUES || '').trim().toLowerCase() === 'off'
const CHAT_LOG = String(process.env.LCZ_CHATLOG || '').trim().toLowerCase() !== 'off'

/* ------------------------------------------------------------ 令牌配置 */
// 优先级：GITHUB_TOKEN 环境变量 > 脚本同目录 config.json > 桌面旧路径兼容
function findConfigToken() {
  const candidates = [
    path.join(SCRIPT_DIR, 'config.json'),
    path.join(os.homedir(), 'Desktop', 'ppe', 'config.json'),
    path.join(os.homedir(), '桌面1', 'ppe', 'config.json'),
  ]
  for (const p of candidates) {
    try {
      const c = JSON.parse(fs.readFileSync(p, 'utf8'))
      if (c && typeof c.GITHUB_TOKEN === 'string' && c.GITHUB_TOKEN.trim()) {
        return { token: c.GITHUB_TOKEN.trim(), from: p }
      }
    } catch (_) {}
  }
  return { token: '', from: '' }
}

const configFound = findConfigToken()
let GITHUB_TOKEN = (process.env.GITHUB_TOKEN || '').trim() || configFound.token

/* ------------------------------------------------------------ 本机状态 */
let state = { requests: [], openRooms: [], processed: [] }

function loadState() {
  try {
    const s = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'))
    if (s && typeof s === 'object') {
      if (Array.isArray(s.requests)) state.requests = s.requests.filter(r => r && r.id && r.room)
      if (Array.isArray(s.openRooms)) state.openRooms = s.openRooms.filter(r => r && r.room)
      if (Array.isArray(s.processed)) state.processed = s.processed.filter(x => typeof x === 'string')
    }
  } catch (_) {}
}

function persistState() {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8')
  } catch (e) {
    console.log('[状态] 写入 rooms.json 失败: ' + (e && e.message))
  }
}

/* ------------------------------------------------------------ 工具 */
function sign(room, act) {
  return crypto.createHash('sha256').update(CONTROL_KEY + '|' + room + '|' + act).digest('hex').slice(0, 32)
}

function sanitizeRoom(raw) {
  return String(raw || '').trim().toLowerCase().replace(/[^a-z0-9_.-]/g, '').slice(0, 40) || 'global'
}

function fmtTime(t) {
  try { return new Date(t).toLocaleString('zh-CN') } catch (_) { return String(t) }
}

function markProcessed(id) {
  if (state.processed.includes(id)) return
  state.processed.push(id)
  if (state.processed.length > PROCESSED_CAP) state.processed = state.processed.slice(-PROCESSED_CAP)
}

function isOpen(room) {
  return state.openRooms.some(r => r.room === room)
}

function setOpen(room, by) {
  const i = state.openRooms.findIndex(r => r.room === room)
  if (i >= 0) {
    const cur = state.openRooms[i]
    if (cur.by === by) return false
    cur.by = by
    cur.t = Date.now()
    return true
  }
  state.openRooms.push({ room, by, t: Date.now() })
  return true
}

function setClosed(room) {
  const i = state.openRooms.findIndex(r => r.room === room)
  if (i < 0) return false
  state.openRooms.splice(i, 1)
  return true
}

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms))
}

/* ------------------------------------------------------------ MQTT */
const client = mqtt.connect(BROKERS[0], {
  clientId: 'lcz-gw-' + crypto.randomBytes(4).toString('hex'),
  reconnectPeriod: 3000,
  connectTimeout: 15000,
})

let mqttUp = false

client.on('connect', () => {
  mqttUp = true
  console.log('[MQTT] 已连接 ' + BROKERS[0])
  client.subscribe([
    ROOM_PREFIX + '+/request',
    ROOM_PREFIX + '+/control',
    ROOM_PREFIX + '+/chat',
  ], { qos: 1 }, (err) => {
    console.log(err ? '[MQTT] 订阅失败: ' + err.message : `[MQTT] 已订阅 ${ROOM_PREFIX}<房间>/{request,control,chat}（retained 请求会自动补收到）`)
  })
})

client.on('reconnect', () => { mqttUp = false; console.log('[MQTT] 重连中……') })
client.on('close', () => { mqttUp = false; console.log('[MQTT] 连接断开，稍后自动重连') })
client.on('error', (e) => { mqttUp = false; console.log('[MQTT] 错误: ' + (e && e.message)) })

client.on('message', (topic, payload) => {
  const str = payload.toString()
  if (!str) return
  const parts = topic.split('/')
  // succession/<room>/<kind>
  if (parts.length !== 3 || parts[0] !== 'succession') return
  const room = sanitizeRoom(parts[1] || '')
  const kind = parts[2]
  if (kind === 'request') {
    handleRequest(room, str).catch(e => console.log('[申请] 处理失败: ' + (e && e.message)))
  } else if (kind === 'control') {
    handleControl(room, str)
  } else if (kind === 'chat' && CHAT_LOG) {
    logChat(room, str)
  }
})

/* ------------------------------------------------------------ 房间控制 */

// 发布房间开关（retained 保留消息：后来者一进房就知道状态）
function publishControl(room, open, by) {
  if (!client) return
  const payload = JSON.stringify({
    v: 1,
    open,
    by,                     // paid = 付费自动 / admin = 管理员
    name: by === 'admin' ? '科学家' : '',
    room,
    k: CONTROL_KEY,
    t: Date.now(),
  })
  client.publish(ROOM_PREFIX + room + '/control', payload, { qos: 1, retain: true }, (err) => {
    console.log('[控制] 房间 ' + room + (err ? ' 发布失败: ' + err.message : (open ? ' → 已发布「开启」' : ' → 已发布「关闭」')))
  })
}

// 收到任何一方的控制消息（含付费自动开房）：同步本机镜像
function handleControl(room, str) {
  let c
  try { c = JSON.parse(str) } catch (_) { return }
  if (!c || typeof c.open !== 'boolean' || c.k !== CONTROL_KEY) return
  if (c.open) {
    const by = c.by === 'paid' ? 'paid' : 'admin'
    if (setOpen(room, by)) {
      persistState()
      console.log('[状态] 房间 ' + room + (by === 'paid' ? ' 已由付费订阅自动开启' : ' 已开启（' + (c.name || '管理员') + '）'))
    }
    // 房间已开启 → 自动消化遗留申请
    resolveByRoom(room)
  } else {
    if (setClosed(room)) {
      persistState()
      console.log('[状态] 房间 ' + room + ' 已关闭')
    }
  }
}

// 开启本机记录的房间（管理员操作）：发保留消息 + 记状态
function approveRoom(room) {
  publishControl(room, true, 'admin')
  setOpen(room, 'admin')
  persistState()
  resolveByRoom(room)
}

function closeRoom(room) {
  publishControl(room, false, 'admin')
  if (setClosed(room)) persistState()
}

/* ------------------------------------------------------------ 开启申请 */

async function handleRequest(room, str) {
  let r
  try { r = JSON.parse(str) } catch (_) { return }
  if (!r || r.v !== 1) return
  if (typeof r.id !== 'string' || !r.id || r.id.length > 64) return
  if (!r.name || typeof r.name !== 'string' || r.name.length > 30) return
  if (state.processed.includes(r.id)) return

  const now = Date.now()
  if (typeof r.t !== 'number' || !(r.t > 0)) r.t = now
  if (now - r.t > REQUEST_TTL_MS) {
    markProcessed(r.id)
    clearRequestTopic(room)
    console.log('[申请] 房间 ' + room + ' 有一条过期申请（超 14 天），已忽略并清除')
    return
  }

  // 去重：同 id 已在待处理列表就补一条 local 日志即可
  const existed = state.requests.find(x => x.id === r.id)
  if (!existed) {
    state.requests.push({
      id: r.id,
      room,
      name: r.name.slice(0, 20),
      text: String(r.text || '').slice(0, 500),
      t: r.t,
      issue: 0,
    })
  }
  persistState()

  const item = existed || state.requests[state.requests.length - 1]
  if (existed) {
    console.log('[申请] 房间 ' + room + ' 的申请 ' + r.id + ' 已在待处理列表（retained 重放，跳过）')
    return
  }

  console.log('============================================================')
  console.log('[申请]  房间 ' + room + ' · ' + item.name + ' · ' + fmtTime(item.t))
  console.log('[留言]  ' + (item.text || '（无）'))
  console.log('[处理]  面板 ' + LOCAL_BASE + ' ｜ 一键开启 ' + LOCAL_BASE + '/api/open?room=' + room + '&k=' + sign(room, 'open'))
  console.log('============================================================')

  if (isOpen(room)) {
    // 房间本来就已开启（比如 delay 的 retained）→ 直接消化
    resolveByRoom(room)
    return
  }
  await notifyGitHub(item)
}

// 房间开启后自动消化该房所有待处理申请：清 retained + 关 Issue + 记 processed
function resolveByRoom(room) {
  if (!isOpen(room)) return
  const pending = state.requests.filter(r => r.room === room)
  for (const it of pending) {
    markProcessed(it.id)
    if (it.issue) closeIssue(it)
  }
  if (pending.length) {
    state.requests = state.requests.filter(r => r.room !== room)
    persistState()
    console.log('[申请] 房间 ' + room + ' 已开启，自动消化 ' + pending.length + ' 条遗留申请')
  }
  // 无论有没有遗留，都确认清掉 retained request（会话无需再处理）
  clearRequestTopic(room)
}

function clearRequestTopic(room) {
  if (!client) return
  client.publish(ROOM_PREFIX + room + '/request', '', { qos: 1, retain: true })
}

function denyRequest(id) {
  const i = state.requests.findIndex(r => r.id === id)
  if (i < 0) return false
  const item = state.requests[i]
  state.requests.splice(i, 1)
  markProcessed(item.id)
  if (item.issue) closeIssue(item)
  clearRequestTopic(item.room)
  persistState()
  console.log('[申请] 房间 ' + item.room + ' · ' + item.name + ' 的申请已拒绝（id=' + item.id + '）')
  return true
}

// 控制台聊天日志（LCZ_CHATLOG=off 关闭）
function logChat(room, str) {
  try {
    const m = JSON.parse(str)
    if (!m || !m.sender) return
    let preview = ''
    if (m.img != null) preview = '[图片' + (m.text ? ' · ' + String(m.text).slice(0, 60) : '') + ']'
    else preview = String(m.text || '').slice(0, 120)
    console.log('[聊天] ' + room + ' · ' + m.sender + ': ' + preview)
  } catch (_) {}
}

/* ------------------------------------------------------------ GitHub Issue 通知 */

async function ghFetch(url, method, bodyObj) {
  for (let i = 0; i < 3; i++) {
    try {
      const r = await fetch(GITHUB_API + url, {
        method,
        headers: {
          authorization: 'Bearer ' + GITHUB_TOKEN,
          accept: 'application/vnd.github+json',
          'user-agent': 'ppe-room-gateway',
          'content-type': 'application/json',
        },
        body: JSON.stringify(bodyObj || {}),
      })
      if (r.status === 502 || r.status === 503 || r.status === 504) {
        await sleep(1400)
        continue
      }
      return r
    } catch (e) {
      if (i === 2) return null
      await sleep(1400)
    }
  }
  return null
}

async function notifyGitHub(item) {
  if (!GITHUB_TOKEN) {
    console.log('[GitHub] 未配置令牌（脚本同目录 config.json 或 GITHUB_TOKEN 环境变量），跳过 Issue 通知；请在面板 ' + LOCAL_BASE + ' 处理')
    return
  }
  const room = item.room
  const openUrl = LOCAL_BASE + '/api/open?room=' + encodeURIComponent(room) + '&k=' + sign(room, 'open')
  const denyUrl = LOCAL_BASE + '/api/deny?id=' + encodeURIComponent(item.id) + '&k=' + sign(room, 'deny')

  const title = '【房间申请开启】房间 ' + room + ' · ' + item.name + ' · ' + fmtTime(item.t)
  const body = [
    '## 房间开启申请',
    '',
    '- **房间号**：`' + room + '`',
    '- **昵称**：' + item.name,
    '- **时间**：' + fmtTime(item.t),
    '- **留言**：' + (item.text || '（无）'),
    '',
    '### 一键处理（须在网关本机的浏览器中点击才有效）',
    '',
    '- ✅ **开启房间**：' + openUrl,
    '- ❌ **拒绝申请**：' + denyUrl,
    '- 🖥 **管理面板**：' + LOCAL_BASE,
    '',
    '> 链接指向本机 127.0.0.1:' + PORT + '，只是在 GitHub 网页里点击无效；',
    '> 拒绝后房间保持关闭，申请者可再次申请；开启后会自动关闭本 Issue。',
  ].join('\n')

  if (TEST_NO_ISSUES) {
    console.log('[测试] LCZ_ISSUES=off → 跳过创建真实 Issue')
    console.log('[测试] 本应创建：' + title)
    return
  }

  const r = await ghFetch('/repos/' + GITHUB_REPO + '/issues', 'POST', { title, body })
  if (r && r.ok) {
    const j = await r.json()
    item.issue = j.number
    item.issueUrl = j.html_url
    persistState()
    console.log('[GitHub] 已创建 Issue #' + j.number + ' → ' + j.html_url)
  } else {
    const msg = r ? 'http=' + r.status : '网络请求失败'
    console.log('[GitHub] 创建 Issue 失败（' + msg + '），仍可在面板处理')
  }
}

async function closeIssue(item) {
  if (!item || !item.issue) return
  if (TEST_NO_ISSUES) return
  const r = await ghFetch('/repos/' + GITHUB_REPO + '/issues/' + item.issue, 'PATCH', { state: 'closed' })
  if (r && r.ok) console.log('[GitHub] Issue #' + item.issue + ' 已关闭')
  else console.log('[GitHub] 关闭 Issue #' + item.issue + ' 失败（可手动关闭）')
}

/* ------------------------------------------------------------ HTTP 面板 */

function sendHtml(res, code, html) {
  res.writeHead(code, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' })
  res.end(html)
}

function sendJson(res, obj) {
  res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' })
  res.end(JSON.stringify(obj))
}

function resultPage(title, detail, redirectSec) {
  const redir = redirectSec ? '<meta http-equiv="refresh" content="' + redirectSec + '; url=/">' : ''
  return '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">' + redir +
    '<title>房间管理网关</title><style>body{font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;background:#0f172a;color:#e2e8f0;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}div{max-width:520px;text-align:center}h2{font-size:18px;margin-bottom:10px}p{font-size:13px;color:#94a3b8;line-height:1.8}a{color:#60a5fa}</style></head><body><div><h2>' + title +
    '</h2><p>' + detail + '</p><p><a href="/">← 返回管理面板</a>' + (redirectSec ? '（' + redirectSec + ' 秒后自动返回）' : '') + '</p></div></body></html>'
}

function listData() {
  return {
    now: Date.now(),
    mqtt: mqttUp,
    token: !!GITHUB_TOKEN,
    issues: TEST_NO_ISSUES ? 'off' : 'on',
    key: CONTROL_KEY,
    pending: state.requests.map(r => ({
      id: r.id,
      room: r.room,
      name: r.name,
      text: r.text,
      t: r.t,
      issue: r.issue || 0,
      openUrl: LOCAL_BASE + '/api/open?room=' + encodeURIComponent(r.room) + '&k=' + sign(r.room, 'open'),
      denyUrl: LOCAL_BASE + '/api/deny?id=' + encodeURIComponent(r.id) + '&k=' + sign(r.room, 'deny'),
    })),
    open: state.openRooms.slice().sort((a, b) => b.t - a.t).map(r => ({
      room: r.room,
      by: r.by,
      t: r.t,
      closeUrl: LOCAL_BASE + '/api/close?room=' + encodeURIComponent(r.room) + '&k=' + sign(r.room, 'close'),
    })),
  }
}

function panelHtml() {
  const data = JSON.stringify({ key: CONTROL_KEY })
  return `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>房间管理网关 · ppe</title>
<style>
  body { margin:0; background:#0f172a; color:#e2e8f0; font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif; }
  header { padding:14px 22px; border-bottom:1px solid #1e293b; display:flex; align-items:baseline; gap:14px; flex-wrap:wrap }
  h1 { font-size:17px; margin:0 }
  #stat { font-size:12px; color:#94a3b8 }
  main { max-width:780px; margin:0 auto; padding:18px 16px 70px }
  section { background:#111827; border:1px solid #1f2937; border-radius:10px; padding:14px 16px; margin-bottom:16px }
  h2 { font-size:14px; margin:0 0 10px; color:#cbd5e1 }
  .row { background:#0b1220; border:1px solid #1e293b; border-radius:8px; padding:10px 12px; margin-bottom:8px }
  .rt { font-size:13px; color:#e2e8f0 }
  .rs { font-size:12px; color:#94a3b8; margin-top:3px }
  .rq { font-size:12px; color:#b6c2d4; background:#0f172a; border-left:3px solid #334155; margin-top:6px; padding:6px 8px; white-space:pre-wrap; word-break:break-word }
  button { border:0; border-radius:6px; padding:6px 12px; font-size:12px; cursor:pointer }
  .b-ok { background:#16a34a; color:#fff }
  .b-no { background:#475569; color:#fff }
  .b-cl { background:#dc2626; color:#fff }
  .btns { margin-top:8px; display:flex; gap:8px; flex-wrap:wrap }
  .empty { font-size:12px; color:#64748b }
  .form { display:flex; gap:8px }
  .form input { flex:1; background:#0b1220; border:1px solid #1e293b; border-radius:6px; color:#e2e8f0; padding:8px 10px; font-size:13px; min-width:0 }
  .tip { color:#64748b; font-size:11px; display:block; margin-top:8px; line-height:1.8 }
</style>
</head>
<body>
<header>
  <h1>房间管理网关</h1>
  <span id="stat">加载中……</span>
</header>
<main>
  <section>
    <h2>待处理申请</h2>
    <div id="pend"><div class="empty">加载中……</div></div>
  </section>
  <section>
    <h2>已开启房间</h2>
    <div id="opens"><div class="empty">加载中……</div></div>
  </section>
  <section>
    <h2>手动开启指定房间</h2>
    <div class="form">
      <input id="mroom" type="text" placeholder="房间号（字母数字_.-，默认 global）" autocomplete="off">
      <button class="b-ok" onclick="manualOpen()">开启</button>
    </div>
    <span class="tip">· 开启后房间立即生效（发布保留消息），页面进房即可发言；<br>· 关闭房间后，已付费用户仍可在页面上自行「恢复开启」；<br>· 链接仅在本机有效：GitHub Issue 里的一键链接也必须在你这台电脑的浏览器里点击。</span>
  </section>
</main>
<script>
const KEY = ${data.key};
function esc(s){ return String(s).replace(/[&<>"']/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
function fmtT(t){ try { return new Date(t).toLocaleString('zh-CN'); } catch(e){ return String(t); } }
async function sha(str){
  const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
  return Array.from(new Uint8Array(b)).map(function(x){ return x.toString(16).padStart(2,'0'); }).join('').slice(0,32);
}
async function manualOpen(){
  const v = document.getElementById('mroom').value.trim().toLowerCase().replace(/[^a-z0-9_.-]/g,'').slice(0,40) || 'global';
  const k = await sha(KEY + '|' + v + '|open');
  location.href = '/api/open?room=' + encodeURIComponent(v) + '&k=' + k;
}
document.addEventListener('click', function(e){
  const b = e.target.closest('button[data-url]');
  if (!b) return;
  if (b.dataset.confirm && !window.confirm(b.dataset.confirm)) return;
  location.href = b.dataset.url;
});
async function refresh(){
  let j;
  try { j = await (await fetch('/api/list')).json(); } catch(e){ j = null; }
  if (!j) return;
  document.getElementById('stat').textContent =
    'MQTT ' + (j.mqtt ? '已连接' : '未连接') +
    ' · GitHub 令牌 ' + (j.token ? '已配置' : '未配置') +
    (j.issues === 'off' ? ' · Issue 测试模式（不创建）' : ' · Issue 通知开') +
    ' · 待处理 ' + j.pending.length + ' · 已开 ' + j.open.length;
  const pend = document.getElementById('pend');
  const open2 = document.getElementById('opens');
  if (!j.pending.length) pend.innerHTML = '<div class="empty">暂无申请，有人提交后会出现在这里（也会收到 GitHub Issue 通知）。</div>';
  else {
    pend.innerHTML = j.pending.map(function(r){
      return '<div class="row">' +
        '<div class="rt">房间 <b>' + esc(r.room) + '</b> · 申请人 ' + esc(r.name) + '</div>' +
        '<div class="rs">' + fmtT(r.t) + (r.issue ? ' · Issue <a style="color:#60a5fa" target="_blank" href="https://github.com/lianchuzhong/ppe/issues/' + r.issue + '">#' + r.issue + '</a>' : '') + '</div>' +
        (r.text ? '<div class="rq">' + esc(r.text) + '</div>' : '') +
        '<div class="btns">' +
        '<button class="b-ok" data-url="' + esc(r.openUrl) + '">✅ 开启房间</button>' +
        '<button class="b-no" data-url="' + esc(r.denyUrl) + '" data-confirm="拒绝这条申请？房间保持关闭，申请人可再次申请。">❌ 拒绝</button>' +
        '</div></div>';
    }).join('');
  }
  if (!j.open.length) open2.innerHTML = '<div class="empty">暂无开启的房间。所有人进房默认「未开启」。</div>';
  else {
    open2.innerHTML = j.open.map(function(r){
      const by = r.by === 'paid' ? '付费订阅自动开启' : (r.by === 'admin' ? '管理员开启' : esc(r.by));
      return '<div class="row">' +
        '<div class="rt">房间 <b>' + esc(r.room) + '</b> <span class="rs">· ' + by + '</span></div>' +
        '<div class="rs">' + fmtT(r.t) + '</div>' +
        '<div class="btns"><button class="b-cl" data-url="' + esc(r.closeUrl) + '" data-confirm="关闭房间「' + esc(r.room) + '」？已付费用户可在页面上自行恢复开启。">⛔ 关闭房间</button></div>' +
        '</div>';
    }).join('');
  }
}
setInterval(refresh, 4000);
refresh();
</script>
</body>
</html>`
}

const server = http.createServer((req, res) => {
  let u
  try { u = new URL(req.url, LOCAL_BASE) } catch (_) { return sendHtml(res, 400, resultPage('请求不合法', '', 0)) }
  const p = u.pathname
  res.setHeader('Cache-Control', 'no-store')

  if (p === '/favicon.ico') { res.writeHead(204); return res.end() }

  if (p === '/' || p === '/index.html') return sendHtml(res, 200, panelHtml())

  if (p === '/api/list') return sendJson(res, listData())

  if (p === '/api/open' || p === '/api/close') {
    const act = p === '/api/open' ? 'open' : 'close'
    const room = sanitizeRoom(u.searchParams.get('room'))
    const k = String(u.searchParams.get('k') || '')
    if (k !== sign(room, act)) return sendHtml(res, 403, resultPage('⛔ 校验失败', '这是一个仅限本机的管理接口，请从管理面板或 Issue 里的链接点击。', 0))
    if (act === 'open') {
      approveRoom(room)
      return sendHtml(res, 200, resultPage('✅ 房间「' + room + '」已开启', '已发布开启状态，进房的人现在可以发言了。', 2))
    }
    closeRoom(room)
    return sendHtml(res, 200, resultPage('⛔ 房间「' + room + '」已关闭', '房间恢复默认关闭状态。已付费用户仍可在页面上自行恢复开启。', 2))
  }

  if (p === '/api/deny') {
    const id = String(u.searchParams.get('id') || '')
    const k = String(u.searchParams.get('k') || '')
    const item = state.requests.find(r => r.id === id)
    if (!item) return sendHtml(res, 404, resultPage('ℹ️ 申请不存在或已处理', '这条申请可能已被开启开启时自动消化，无需再处理。', 3))
    if (k !== sign(item.room, 'deny')) return sendHtml(res, 403, resultPage('⛔ 校验失败', '这是一个仅限本机的管理接口，请从管理面板或 Issue 里的链接点击。', 0))
    denyRequest(id)
    return sendHtml(res, 200, resultPage('已拒绝该申请', '房间保持关闭；申请人之后可再次提交。', 3))
  }

  return sendHtml(res, 404, resultPage('404', '接口不存在。可用接口：<br>http://127.0.0.1:' + PORT + '/（面板）', 0))
})

server.on('error', (e) => {
  console.log('[HTTP] 端口 ' + PORT + ' 不可用（' + (e && e.code) + '）：可能已有网关在运行，本进程退出')
  process.exit(1)
})

/* ------------------------------------------------------------ 启动 */

process.on('unhandledRejection', (e) => console.log('[异常] unhandledRejection: ' + ((e && e.message) || e)))
process.on('uncaughtException', (e) => console.log('[异常] uncaughtException: ' + ((e && e.message) || e)))

loadState()

if (TEST_NO_ISSUES) {
  console.log('*** 测试模式 LCZ_ISSUES=off：不会创建真实 GitHub Issue ***')
}
if (CHAT_LOG) {
  console.log('*** 聊天日志已开启（LCZ_CHATLOG=off 可关闭）***')
}

console.log('[网关] 状态文件: ' + STATE_FILE)
console.log('[网关] GitHub 令牌: ' + (GITHUB_TOKEN ? '已配置（' + ((process.env.GITHUB_TOKEN ? '环境变量' : configFound.from) || 'config.json') + '）' : '未配置（GitHub 通知不可用，仍可用面板处理）'))
console.log('[网关] 待处理申请 ' + state.requests.length + ' 条 · 已开房间 ' + state.openRooms.length + ' 个 · 已处理 id ' + state.processed.length + ' 个')

server.listen(PORT, '127.0.0.1', () => {
  console.log('[网关] 面板已就绪 → ' + LOCAL_BASE)
  console.log('[网关] 工作流：有人申请开启房间 → MQTT request → 本网关建 GitHub Issue 通知你 → 面板/一键链接开启')
})
