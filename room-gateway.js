import mqtt from 'mqtt'
import http from 'node:http'
import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import os from 'node:os'
import { fileURLToPath } from 'node:url'

/* ================================================================
 * ppe 房间管理网关 v2 —— 私库管理中枢
 *
 *  1. 免费申请（retained request）→ 私库 ppe-private 建 Issue 通知科学家
 *  2. 面板 http://127.0.0.1:8899 处理申请 / 开关房间（操作回写私库）
 *  3. 私库 rooms.json 是房间开关的「唯一权威」：网关每 30s 轮询自动应用
 *     （付费房 by=paid 受保护：谁也关不掉 —— 用户明确要求）
 *  4. 聊天内容：实时落桌面「聊天记录/YYYY-MM-DD.jsonl」+ 定时归档私库
 *     chatlogs/YYYY-MM-DD.jsonl（图片存标记，节省空间）
 *  5. 付费事件 → supporters.json 登记 + 上报多网站支持者库（fail-soft）
 *  6. 所有开关/拒绝/付费动作 → 桌面 audit-*.json + 私库 audit.jsonl 流水
 *
 * 测试开关：
 *   LCZ_ISSUES=off  不建真实 Issue（两个库都不建）
 *   LCZ_PRIVATE=off 不读写私库（本地纯面板测试）
 *   LCZ_POLL_SEC / LCZ_ARCHIVE_SEC  轮询/归档周期（默认 30s / 300s）
 * ================================================================ */

const BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt',
]
// LCZ_PFX / LCZ_PORT 仅测试期使用：换主题前缀/端口，避免测试流量打进生产订阅
const ROOM_PREFIX = (() => {
  const p = String(process.env.LCZ_PFX || 'succession/')
  return p.endsWith('/') ? p : p + '/'
})()
const PORT = Math.max(1024, parseInt(process.env.LCZ_PORT || '8899', 10) || 8899)
const LOCAL_BASE = 'http://127.0.0.1:' + PORT
// 主站点仓库（旧版兼容：不再往这里建 issue）
const PUBLIC_REPO = 'lianchuzhong/ppe'
// 私库：开关权威 / 聊天归档 / 支持者 / 审计 / 申请通知
const PRIVATE_REPO_DEFAULT = 'lianchuzhong/ppe-private'
// 多网站支持者接口（short-video worker， Pay 校验 ACTIVE 后入库；secrets 未配时 fail-soft）
const SUP_WORKER_DEFAULT = 'https://lcz-video-alerts.lcz-video-alerts.workers.dev'
const GITHUB_API = 'https://api.github.com'

// 与页面 src/app.js 保持一致（防路人误发布，公开页面防误触不防恶意）
const CONTROL_KEY = 'lcz-room-official-9f3a2c'

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url))
const STATE_FILE = path.join(SCRIPT_DIR, 'rooms.json')
const AUDIT_FILE = path.join(SCRIPT_DIR, 'audit-local.jsonl')
const CHAT_DIR = path.join(SCRIPT_DIR, '聊天记录')
const PROCESSED_CAP = 400
const REQUEST_TTL_MS = 14 * 24 * 3600 * 1000

const TEST_NO_ISSUES = String(process.env.LCZ_ISSUES || '').trim().toLowerCase() === 'off'
const PRIVATE_DISABLE = String(process.env.LCZ_PRIVATE || '').trim().toLowerCase() === 'off'
const CHAT_LOG = String(process.env.LCZ_CHATLOG || '').trim().toLowerCase() !== 'off'
const POLL_SEC = Math.max(5, parseInt(process.env.LCZ_POLL_SEC || '30', 10) || 30)
const ARCHIVE_SEC = Math.max(5, parseInt(process.env.LCZ_ARCHIVE_SEC || '300', 10) || 300)

/* ------------------------------------------------------------ 配置 */
function loadConfig() {
  const candidates = [
    path.join(SCRIPT_DIR, 'config.json'),
    path.join(os.homedir(), 'Desktop', 'ppe', 'config.json'),
    path.join(os.homedir(), '桌面1', 'ppe', 'config.json'),
  ]
  for (const p of candidates) {
    try {
      const c = JSON.parse(fs.readFileSync(p, 'utf8'))
      if (c && typeof c.GITHUB_TOKEN === 'string' && c.GITHUB_TOKEN.trim()) {
        return { token: c.GITHUB_TOKEN.trim(), from: p, extra: c }
      }
    } catch (_) {}
  }
  return { token: '', from: '', extra: {} }
}

const configFound = loadConfig()
const envToken = (process.env.GITHUB_TOKEN || '').trim()
let GITHUB_TOKEN = envToken || configFound.token
const PRIVATE_REPO = String(configFound.extra.PRIVATE_REPO || PRIVATE_REPO_DEFAULT).trim()
const SUP_WORKER = String(configFound.extra.SUP_WORKER || SUP_WORKER_DEFAULT).replace(/\/+$/, '')

function privateEnabled() {
  return !PRIVATE_DISABLE && PRIVATE_OK === true
}

/* ------------------------------------------------------------ 本机状态 */
let state = { requests: [], openRooms: [], processed: [], knownRooms: [], visitIssue: null }
let supporterIds = new Set()           // 已登记过的订阅号（首次付费才发通知 Issue）
let lastRoomsHash = ''                 // 最近一次应用的私库 rooms.json 内容指纹（防自身回环）
let roomsEtag = ''                     // If-None-Match
let PRIVATE_OK = null                  // null=未探测 true/false
let mqttUp = false
let chatDirtyDays = new Set()          // 今天有新聊天的日期（等归档）

function loadState() {
  try {
    const s = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'))
    if (s && typeof s === 'object') {
      if (Array.isArray(s.requests)) state.requests = s.requests.filter(r => r && r.id && r.room)
      if (Array.isArray(s.openRooms)) state.openRooms = s.openRooms.filter(r => r && r.room)
      if (Array.isArray(s.processed)) state.processed = s.processed.filter(x => typeof x === 'string')
      if (Array.isArray(s.knownRooms)) state.knownRooms = s.knownRooms.filter(x => typeof x === 'string')
      if (s.visitIssue && s.visitIssue.number) state.visitIssue = { number: s.visitIssue.number, url: s.visitIssue.url || '', t: s.visitIssue.t || 0 }
      if (s.lastRoomsHash) { lastRoomsHash = s.lastRoomsHash }
    }
  } catch (_) {}
}

function persistState() {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify({
      requests: state.requests,
      openRooms: state.openRooms,
      processed: state.processed,
      knownRooms: state.knownRooms,
      visitIssue: state.visitIssue,
      lastRoomsHash,
    }, null, 2), 'utf8')
  } catch (e) {
    console.log('[状态] 写入 rooms.json 失败: ' + (e && e.message))
  }
}

/* ------------------------------------------------------------ 工具 */
function sign(room, act) {
  return crypto.createHash('sha256').update(CONTROL_KEY + '|' + room + '|' + act).digest('hex').slice(0, 32)
}

function sha1hex(s) {
  return crypto.createHash('sha1').update(String(s || ''), 'utf8').digest('hex')
}

function sanitizeRoom(raw) {
  return String(raw || '').trim().toLowerCase().replace(/[^a-z0-9_.-]/g, '').slice(0, 40) || 'global'
}

function fmtTime(t) {
  try { return new Date(t).toLocaleString('zh-CN') } catch (_) { return String(t) }
}

function dayStr(t) {
  const d = new Date(t || Date.now())
  const p = (n) => String(n).padStart(2, '0')
  return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate())
}

function markProcessed(id) {
  if (state.processed.includes(id)) return
  state.processed.push(id)
  if (state.processed.length > PROCESSED_CAP) state.processed = state.processed.slice(-PROCESSED_CAP)
}

function isOpen(room) {
  return state.openRooms.some(r => r.room === room)
}

function openRoomBy(room) {
  const r = state.openRooms.find(x => x.room === room)
  return r ? r.by : null
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

/* ------------------------------------------------------------ 本机审计流水 */
function auditLocal(act, data) {
  const line = JSON.stringify(Object.assign({ t: Date.now(), act }, data || {}))
  try { fs.appendFileSync(AUDIT_FILE, line + '\n', 'utf8') } catch (_) {}
  return line
}

/* ------------------------------------------------------------ GitHub API */
async function ghFetch(url, method, bodyObj, extraHeaders) {
  if (!GITHUB_TOKEN) return null
  for (let i = 0; i < 3; i++) {
    try {
      const r = await fetch(GITHUB_API + url, {
        method,
        headers: Object.assign({
          authorization: 'Bearer ' + GITHUB_TOKEN,
          accept: 'application/vnd.github+json',
          'user-agent': 'ppe-room-gateway',
          'content-type': 'application/json',
        }, extraHeaders || {}),
        body: bodyObj ? JSON.stringify(bodyObj) : undefined,
        signal: AbortSignal.timeout(15000),   // 防 GFW 黑洞连接：单请求 15s 封顶，超时即走重试
      })
      if (r.status === 502 || r.status === 503 || r.status === 504) { await sleep(1400); continue }
      return r
    } catch (e) {
      if (i === 2) return null
      await sleep(1400)
    }
  }
  return null
}

async function createIssue(repo, title, body) {
  if (TEST_NO_ISSUES) {
    console.log('[测试] LCZ_ISSUES=off → 跳过创建 Issue: ' + title)
    return null
  }
  const r = await ghFetch('/repos/' + repo + '/issues', 'POST', { title, body })
  if (r && r.ok) {
    const j = await r.json()
    console.log('[Issue] ' + repo + ' #' + j.number + ' → ' + (j.html_url || '').replace(/\/\d+$/, '/#' + j.number))
    return j
  }
  console.log('[Issue] ' + repo + ' 创建失败' + (r ? ' http=' + r.status : '（网络）'))
  return null
}

async function closeIssue(repo, issueNo) {
  if (TEST_NO_ISSUES || !issueNo) return
  const r = await ghFetch('/repos/' + repo + '/issues/' + issueNo, 'PATCH', { state: 'closed' })
  if (r && r.ok) console.log('[Issue] ' + repo + ' #' + issueNo + ' 已关闭')
  else console.log('[Issue] 关闭 ' + repo + ' #' + issueNo + ' 失败（可手动关闭）')
}

/* ------------------------------------------------------------ 私库基础读写 */
async function probePrivate() {
  if (PRIVATE_DISABLE) { PRIVATE_OK = false; return false }
  if (!GITHUB_TOKEN) { PRIVATE_OK = false; return false }
  const r = await ghFetch('/repos/' + PRIVATE_REPO)
  PRIVATE_OK = !!(r && r.ok)
  console.log(privateEnabled()
    ? '[私库] ' + PRIVATE_REPO + ' 已连接（开关权威/聊天归档/审计）'
    : '[私库] ' + PRIVATE_REPO + ' 不可用' + (r ? ' http=' + r.status : '（无令牌或网络）') + '，私库功能停用地端面板')
  return privateEnabled()
}

// 读私库文件：返回 {raw, sha} 或 {raw:'', sha:null}(不存在) / null(不可用)。瞬态错误自动重试
async function readPrivateFile(p) {
  if (!privateEnabled()) return null
  const url = '/repos/' + PRIVATE_REPO + '/contents/' + p + '?ref=main'
  for (let i = 0; i < 3; i++) {
    const r = await ghFetch(url)
    if (r === null) return null
    if (r.status === 404) return { raw: '', sha: null }
    if (r.ok) {
      const j = await r.json()
      const raw = Buffer.from(String(j.content || '').replace(/\n/g, ''), 'base64').toString('utf8')
      return { raw, sha: j.sha }
    }
    if (i < 2) await sleep(1800)   // 403/429 限速等瞬态：稍等重试
  }
  console.log('[私库] 读 ' + p + ' 连续失败（网络/API）')
  return null
}

// 写私库文件（有 sha 就更新，没有就新建）；失败总体重试一次
async function writePrivateFile(p, content, message, retried) {
  if (!privateEnabled()) return false
  const cur = await readPrivateFile(p)
  if (cur === null) { console.log('[私库] 写 ' + p + ' 失败：读当前内容不可用（网络/API）'); return false }
  if (cur.raw === content) return true   // 内容一致：幂等跳过，省 commit
  const body = {
    message: message || ('更新 ' + p),
    content: Buffer.from(content, 'utf8').toString('base64'),
    branch: 'main',
  }
  if (cur.sha) body.sha = cur.sha
  const r = await ghFetch('/repos/' + PRIVATE_REPO + '/contents/' + p, 'PUT', body)
  if (r && r.ok) return true
  // 并发冲突重试一次
  if (r && r.status === 409) {
    const again = await readPrivateFile(p)
    if (again && again.sha) {
      body.sha = again.sha
      const r2 = await ghFetch('/repos/' + PRIVATE_REPO + '/contents/' + p, 'PUT', body)
      if (r2 && r2.ok) return true
    }
  }
  if (!retried) {   // 瞬态失败（限速/冲突）：整体再试一次
    await sleep(1600)
    return writePrivateFile(p, content, message, true)
  }
  console.log('[私库] 写 ' + p + ' 失败' + (r ? ' http=' + r.status : ''))
  return false
}

// 追加一行到私库 jsonl（低频事件：开关/拒绝/付费）
async function appendPrivateJsonl(p, obj) {
  const cur = await readPrivateFile(p)
  if (cur === null) return false
  const next = (cur.raw ? cur.raw.replace(/\n+$/, '') + '\n' : '') + JSON.stringify(obj) + '\n'
  return writePrivateFile(p, next, '审计: ' + (obj.act || obj.kind || 'event'))
}

/* ------------------------------------------------------------ 访客记录（一条长期 Issue，按批汇总） */

const VISIT_TITLE = '📊 访客记录（自动汇总）'
const VISIT_CID_TTL_MS = 30 * 60 * 1000   // 与页面端 cid 保鲜 30 分钟对齐：同一会话重复上报不重复计数
const VISIT_SEEN_CAP = 4000               // 去重表容量兜底
const VISIT_FLUSH_MS = 60 * 1000          // 攒批周期
const VISIT_LINES_PER_COMMENT = 50        // 单条评论最多行数（防超长）

let visitSeen = new Map()   // room|cid -> 最近一次出现时间
let visitTotal = 0          // 本进程启动以来去重后的访客事件数（面板展示用）
let visitRecent = []        // 最近事件（仅内存，面板展示）
let visitBuffer = []        // 待写进 Issue 的记录
let visitIssueEnsuring = null

function handleVisit(room, str) {
  let v
  try { v = JSON.parse(str) } catch (_) { return }
  if (!v || v.v !== 1) return
  const cid = String(v.cid || '')
  if (!/^[a-z0-9-]{6,64}$/.test(cid)) return
  const t = Number(v.t) || 0
  const now = Date.now()
  // 发送端不是 retained，但保险仍做新鲜度校验（同 control 的 2 分钟窗口一致）
  if (!t || t > now + 60000 || now - t > 120000) return
  const key = room + '|' + cid
  const seen = visitSeen.get(key)
  if (seen && now - seen < VISIT_CID_TTL_MS) {
    visitSeen.set(key, now)   // 同一会话继续在线：滚动保鲜，不重复计数
    return
  }
  visitSeen.set(key, now)
  if (visitSeen.size > VISIT_SEEN_CAP) {
    const arr = [...visitSeen.entries()].sort((a, b) => a[1] - b[1])
    for (let i = 0; i < Math.floor(arr.length / 2); i++) visitSeen.delete(arr[i][0])
  }
  visitTotal++
  const label = room === '_meta' ? '主页' : '房间 ' + room
  console.log('[访客] ' + label + ' · 会话 ' + cid.slice(-6))
  auditLocal('visit', { room, cid: cid.slice(-6) })
  visitRecent.unshift({ t: now, room: label, cid: cid.slice(-6) })
  if (visitRecent.length > 30) visitRecent = visitRecent.slice(0, 30)
  visitBuffer.push({ t: now, label, cid: cid.slice(-6) })
  if (visitBuffer.length > 300) visitBuffer = visitBuffer.slice(-300)
}

// 长期汇总 Issue：状态里记过号就用它，否则搜现有开放 Issue（重启防重建），没有才新建
async function ensureVisitIssue() {
  if (state.visitIssue && state.visitIssue.number) return state.visitIssue
  if (visitIssueEnsuring) return visitIssueEnsuring
  visitIssueEnsuring = (async () => {
    try {
      const r = await ghFetch('/repos/' + PRIVATE_REPO + '/issues?state=open&per_page=100&sort=created&direction=asc')
      if (r && r.ok) {
        const list = await r.json()
        const found = Array.isArray(list) ? list.find(it => String(it.title || '').indexOf(VISIT_TITLE) === 0) : null
        if (found) {
          state.visitIssue = { number: found.number, url: found.html_url || '', t: Date.now() }
          persistState()
          console.log('[访客] 复用已有汇总 Issue #' + found.number)
          return state.visitIssue
        }
      }
      const j = await createIssue(PRIVATE_REPO, VISIT_TITLE, [
        '📊 **访客记录（自动汇总）**',
        '',
        '网关每分钟把新出现的访客（按浏览器会话去重）追加到本 Issue 的评论里。',
        '- 「主页」= 只看过登录页、没进任何房间的访客；「房间 x」= 进过该房间的访客',
        '- 「会话尾号」= 浏览器端随机 id 的尾 6 位（30 分钟内反复刷新视为同一次访问，不重复计数）',
        '',
        '> 本条请保持开放；若手动关闭，网关之后会再建一条新的汇总 Issue。',
      ].join('\n'))
      if (j && j.number) {
        state.visitIssue = { number: j.number, url: j.html_url || '', t: Date.now() }
        persistState()
        console.log('[访客] 已创建汇总 Issue #' + j.number)
        return state.visitIssue
      }
      return null
    } finally {
      visitIssueEnsuring = null
    }
  })()
  return visitIssueEnsuring
}

async function flushVisitBuffer() {
  if (!visitBuffer.length) return
  if (!privateEnabled() || TEST_NO_ISSUES) { visitBuffer = []; return }   // 本地测试模式：只留本地审计
  const issue = await ensureVisitIssue()
  if (!issue || !issue.number) return    // 网络失败：留着本批下轮再试
  const batch = visitBuffer.splice(0, VISIT_LINES_PER_COMMENT)
  const lines = ['**本批 ' + batch.length + ' 条 · ' + fmtTime(batch[0].t) + '**', '']
  for (const r of batch) lines.push('- ' + fmtTime(r.t) + ' · ' + r.label + ' · 会话尾号 ' + r.cid)
  const r = await ghFetch('/repos/' + PRIVATE_REPO + '/issues/' + issue.number + '/comments', 'POST', { body: lines.join('\n') })
  if (r && r.ok) {
    console.log('[访客] 已汇总 ' + batch.length + ' 条 → Issue #' + issue.number)
  } else {
    visitBuffer = batch.concat(visitBuffer)   // 失败放回队首，下轮重试
    console.log('[访客] 汇总失败' + (r ? ' http=' + r.status : '（网络）') + '，本批下轮重试')
  }
}

/* ------------------------------------------------------------ 房间控制 */

function publishControl(room, open, by, extra) {
  if (!client) return
  const payload = JSON.stringify(Object.assign({
    v: 1,
    open,
    by,                         // paid = 付费自动 / admin = 管理员
    name: by === 'admin' ? '科学家' : '',
    room,
    k: CONTROL_KEY,
    t: Date.now(),
  }, extra || {}))
  client.publish(ROOM_PREFIX + room + '/control', payload, { qos: 1, retain: true }, (err) => {
    console.log('[控制] 房间 ' + room + (err ? ' 发布失败: ' + err.message : (open ? ' → 已发布「开启」(' + by + ')' : ' → 已发布「关闭」')))
  })
}

function approveRoom(room, via) {
  publishControl(room, true, 'admin')
  const changed = setOpen(room, 'admin')
  if (changed) persistState()
  resolveByRoom(room)
  auditLocal('open-room', { room, via: via || 'admin' })
  auditPrivate('open-room', { room, via: via || 'admin', by: 'admin' })
  syncRoomToPrivate(room, true, 'admin', via || 'admin')
  if (changed) notifyNewRoom(room, via || 'admin').catch(e => console.log('[新房间] 失败: ' + (e && e.message)))
  return true
}

// 关闭受「付费房保护」约束
function closeRoom(room, via) {
  if (openRoomBy(room) === 'paid') {
    console.log('[保护] 付费房间 ' + room + ' 不允许被关闭（用户明确要求）')
    return false
  }
  if (!isOpen(room)) return false   // 没开就什么都不发布：防「自己发布→自己收到→再发布」死循环
  publishControl(room, false, 'admin')
  setClosed(room)
  persistState()
  auditLocal('close-room', { room, via: via || 'admin', by: 'admin' })
  auditPrivate('close-room', { room, via: via || 'admin', by: 'admin' })
  syncRoomToPrivate(room, false, 'admin', via || 'admin')
  return true
}

// 付费自动开房（页面 onApprove/恢复 发出的 by=paid control）
function handlePaidOpen(room, payload) {
  const changed = setOpen(room, 'paid')
  if (!changed) return   // retained 重放/已同状态：静默，不重复审计
  persistState()
  console.log('[状态] 房间 ' + room + ' 已由付费订阅自动开启')
  resolveByRoom(room)
  auditLocal('paid-open', { room, subId: String(payload.subId || '').slice(-6) })
  auditPrivate('paid-open', { room, subId: String(payload.subId || '').slice(-6), by: 'paid' })
  if (payload.subId) notifyNewSupporter({ subId: payload.subId, room, name: payload.name || '', t: payload.t || Date.now() })
  syncRoomToPrivate(room, true, 'paid', 'subscriber')
  notifyNewRoom(room, 'paid').catch(e => console.log('[新房间] 失败: ' + (e && e.message)))
}

async function handleControl(room, str) {
  let c
  try { c = JSON.parse(str) } catch (_) { return }
  if (!c || typeof c.open !== 'boolean' || c.k !== CONTROL_KEY) return
  // retained 重放守卫：超过 2 分钟的旧消息不是新动作（页面进房靠 retained 恢复门状态；
  // 网关的记账/审计/私库同步只认新事件，开关恢复走私库权威 rooms.json）
  const stale = typeof c.t === 'number' && c.t > 0 && (Date.now() - c.t > 120000)
  if (stale) return
  if (c.open) {
    if (c.by === 'paid') {
      handlePaidOpen(room, c)
    } else {
      if (setOpen(room, 'admin')) {
        persistState()
        console.log('[状态] 房间 ' + room + ' 已开启（' + (c.name || '管理员') + '）')
        auditLocal('open-room', { room, via: 'mqtt', by: 'admin' })
        auditPrivate('open-room', { room, via: 'mqtt', by: 'admin' })
        syncRoomToPrivate(room, true, 'admin', 'mqtt')
        notifyNewRoom(room, 'mqtt').catch(e => console.log('[新房间] 失败: ' + (e && e.message)))
      }
      resolveByRoom(room)
    }
  } else {
    closeRoom(room, 'mqtt')   // 走统一关闭逻辑：含付费保护和审计（防绕过）
  }
}

// 房间开启后自动消化该房所有待处理申请：清 retained + 关 Issue + 记 processed
function resolveByRoom(room) {
  if (!isOpen(room)) return
  const pending = state.requests.filter(r => r.room === room)
  for (const it of pending) {
    markProcessed(it.id)
    closeIssue(PRIVATE_REPO, it.issue)
  }
  if (pending.length) {
    state.requests = state.requests.filter(r => r.room !== room)
    persistState()
    console.log('[申请] 房间 ' + room + ' 已开启，自动消化 ' + pending.length + ' 条遗留申请')
  }
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
  closeIssue(PRIVATE_REPO, item.issue)
  clearRequestTopic(item.room)
  persistState()
  auditLocal('deny-request', { room: item.room, id: item.id })
  auditPrivate('deny-request', { room: item.room, id: item.id.slice(-10), name: item.name })
  console.log('[申请] 房间 ' + item.room + ' · ' + item.name + ' 的申请已拒绝（id=' + item.id + '）')
  return true
}

/* ------------------------------------------------------------ 私库：房间开关同步 */

// 私库 rooms.json 轮询（唯一权威）
async function pollRooms() {
  if (!privateEnabled()) return
  const r = await ghFetch('/repos/' + PRIVATE_REPO + '/contents/rooms.json?ref=main', 'GET', null, roomsEtag ? { 'If-None-Match': roomsEtag } : {})
  if (r === null) return
  if (r.status === 304) return
  if (!r.ok) { return }
  try { if (r.headers && r.headers.get) roomsEtag = r.headers.get('etag') || '' } catch (_) {}
  let j = null
  try {
    j = JSON.parse(Buffer.from(String((await r.json()).content || '').replace(/\n/g, ''), 'base64').toString('utf8'))
  } catch (e) {
    console.log('[私库] rooms.json 解析失败: ' + (e && e.message))
    return
  }
  const raw = JSON.stringify(j && j.rooms ? j.rooms : {})
  const h = sha1hex(raw)
  if (h === lastRoomsHash) return    // 没变化（含我们自己刚写回的）
  lastRoomsHash = h                  // 模块级才是比对基准（state.lastRoomsHash 只是持久化镜像）
  persistState()
  await applyRoomsMap(j && j.rooms ? j.rooms : {}, 'private-repo')
}

async function applyRoomsMap(mapObj, src) {
  for (const key of Object.keys(mapObj || {})) {
    const room = sanitizeRoom(key)
    const spec = mapObj[key]
    if (!spec || typeof spec.open !== 'boolean') continue
    const cur = isOpen(room)
    const curBy = openRoomBy(room)
    if (spec.open === true) {
      if (cur) continue   // 已开着（admin 或付费）就不再动作——付费房绝不降级为普通房
      if (spec.by === 'paid') {
        // 私库权威记录这是付费开启的房间：按原样还原（含发布 fresh control 让所有人页面上门恢复）
        console.log('[私库] 按权威记录还原付费开启的房间 ' + room + '（非新事件，不重复审计）')
        publishControl(room, true, 'paid')
        setOpen(room, 'paid')
        persistState()
        markRoomKnown(room)   // 还原不是新开：静默标记，避免开机还原刷「新房间」Issue
        resolveByRoom(room)
      } else {
        console.log('[私库] 依据权威指令开启房间 ' + room + '（来自 ' + src + '）')
        approveRoom(room, src)
      }
    } else {
      if (curBy === 'paid') {
        console.log('[私库] 房间 ' + room + ' 是付费开启，按用户要求不关闭')
        auditLocal('close-skip-paid', { room, src })
        auditPrivate('close-skip-paid', { room, src })
        continue
      }
      if (!cur) continue
      console.log('[私库] 依据权威指令关闭房间 ' + room + '（来自 ' + src + '）')
      closeRoom(room, src)
    }
  }
}

/* ------------------------------------------------------------ 新房间通知（每个房号首次出现才建一条 Issue） */

function markRoomKnown(room) {
  if (state.knownRooms.includes(room)) return false
  state.knownRooms.push(room)
  if (state.knownRooms.length > 500) state.knownRooms = state.knownRooms.slice(-500)
  persistState()
  return true
}

async function notifyNewRoom(room, via) {
  if (!room) return
  if (!markRoomKnown(room)) return   // 老房号：不再打扰
  const viaLabel = via === 'paid' ? '订阅者付费自动开启'
    : via === 'mqtt' ? '管理员（MQTT 控制通道）'
    : via === 'private-repo' ? '私库权威指令（rooms.json）'
    : '管理员（' + (via || '面板') + '）'
  console.log('[新房间] 房间 ' + room + ' 首次出现（' + (via || '未知') + '）')
  auditLocal('new-room', { room, via: via || 'unknown' })
  auditPrivate('new-room', { room, via: via || 'unknown' })
  await createIssue(PRIVATE_REPO, '🆕 新房间开启：' + room, [
    '房间号 `' + room + '` 首次开启。',
    '',
    '- **时间**：' + fmtTime(Date.now()),
    '- **来源**：' + viaLabel,
    '',
    '> 付费开启的房间受保护不会自动关闭；同一房号只在本条 Issue 里通知这一次。',
  ].join('\n'))
}

// 开机基线：openRooms + 私库 rooms.json 里已有的房号都标记为已知（不通知）
async function seedKnownRooms() {
  let added = 0
  for (const it of state.openRooms || []) {
    if (it && it.room && !state.knownRooms.includes(it.room)) { state.knownRooms.push(it.room); added++ }
  }
  if (privateEnabled()) {
    try {
      const cur = await readPrivateFile('rooms.json')
      if (cur && cur.raw) {
        const j = JSON.parse(cur.raw || '{}') || {}
        for (const key of Object.keys(j.rooms || {})) {
          const room = sanitizeRoom(key)
          if (room && !state.knownRooms.includes(room)) { state.knownRooms.push(room); added++ }
        }
      }
    } catch (_) {}
  }
  if (state.knownRooms.length > 500) state.knownRooms = state.knownRooms.slice(-500)
  if (added) persistState()
  console.log('[新房间] 已有房号基线 ' + state.knownRooms.length + ' 个（此后房号新增才建 Issue）')
}

// 面板/网关自己的开关动作 → 回写私库 rooms.json（让私库始终反映当前开关状态；私库键保留 note）
// 串行队列：开/关连续触发时防止「旧合并内容晚到覆盖新状态」的竞态（409 重试会保留原 body 内容）
let syncPrivChain = Promise.resolve()
function syncRoomToPrivate(room, open, by, via) {
  if (!privateEnabled()) return Promise.resolve()
  syncPrivChain = syncPrivChain.then(async () => {
    try {
      const cur = await readPrivateFile('rooms.json')
      if (cur === null) { console.log('[私库] sync rooms.json 失败：读当前内容不可用'); return }
      let j = { rooms: {}, note: '' }
      try { j = JSON.parse(cur.raw || '{}') || {} } catch (_) {}
      if (!j.rooms || typeof j.rooms !== 'object') j.rooms = {}
      j.rooms[room] = Object.assign({}, j.rooms[room] || {}, {
        open: !!open,
        by: by,
        updated: new Date().toISOString(),
        note: (j.rooms[room] && j.rooms[room].note) || '',
        via: via || '',
      })
      // 写回前更新 lastRoomsHash 指纹，避免轮询把自己当新一轮指令
      lastRoomsHash = sha1hex(JSON.stringify(j.rooms || {}))
      persistState()
      const ok = await writePrivateFile('rooms.json', JSON.stringify(j, null, 2),
        '房间开关同步: ' + room + ' = ' + (open ? '开启' : '关闭') + '（' + (via || by) + '）')
      if (!ok) console.log('[私库] sync rooms.json 失败')
    } catch (e) {
      console.log('[私库] syncRoomToPrivate 异常: ' + (e && e.message))
    }
  }).catch(() => {})
  return syncPrivChain
}

// 写私库审计：串行队列（防并发读旧内容互相覆盖），失败只记日志不影响本地
let auditPrivChain = Promise.resolve()
function auditPrivate(act, data) {
  if (!privateEnabled()) return
  auditPrivChain = auditPrivChain
    .then(() => appendPrivateJsonl('audit.jsonl', Object.assign({ t: Date.now(), act, room: data && data.room }, data || {})))
    .catch(e => console.log('[私库] audit.jsonl 追加失败: ' + ((e && e.message) || e)))
  return auditPrivChain
}

/* ------------------------------------------------------------ 支持者（多网站权益） */

const SUB_ID_RE = /^[A-Za-z0-9-]{4,64}$/

// 开机预载：supporters.json 里的已知订阅号（防止重启后把老订阅者当新支持者再发通知）
async function preloadSupporters() {
  if (!privateEnabled()) return
  const cur = await readPrivateFile('supporters.json')
  if (!cur || !cur.raw) return
  try {
    const j = JSON.parse(cur.raw || '{}')
    for (const k of Object.keys(j.supporters || {})) supporterIds.add(k)
    console.log('[支持者] 已登记订阅号 ' + supporterIds.size + ' 个（私库预载）')
  } catch (_) {}
}

// 首次出现的订阅号 → 记入 supporters.json + 通知 Issue；重复出现仅审计
async function notifyNewSupporter(rec) {
  const subId = String(rec.subId || '').trim()
  if (!subId || !SUB_ID_RE.test(subId)) return
  const tail = subId.slice(-6)
  let known = supporterIds.has(subId)
  if (!known && privateEnabled()) {
    const cur = await readPrivateFile('supporters.json')
    if (cur && cur.raw) {
      try {
        const j = JSON.parse(cur.raw || '{}')
        const list = j.supporters || {}
        known = known || Object.prototype.hasOwnProperty.call(list, subId)
        list[subId] = Object.assign({}, list[subId], {
          room: rec.room,
          name: rec.name,
          t: rec.t,
          source: 'ppe',
          updated: new Date().toISOString(),
        })
        supporterIds.add(subId)
        await writePrivateFile('supporters.json', JSON.stringify(j, null, 2), '支持者登记（尾号 ' + tail + '）')
      } catch (_) {}
    }
  }
  supporterIds.add(subId)
  auditPrivate('supporter', { room: rec.room, tail, first: !known })
  // 多网站权益：上报支持者库（short-video worker；fail-soft，secrets 未配或网络不通时放弃）
  registerSupporterRemote(subId, rec)
  if (!known) {
    await createIssue(PRIVATE_REPO,
      '【付费订阅开启】房间 ' + rec.room + ' · 尾号 ' + tail + ' · ' + fmtTime(rec.t),
      '> 该房间已由用户付费订阅自动开启，无需操作。\n\n' +
      '- 房间号：`' + rec.room + '`\n- 订阅尾号：' + tail + '\n- 依据用户要求：付费房间不会被关闭\n- 已登记到 supporters.json，并已尝试同步到多网站支持者库（短视频站同样免费）\n\n处理面板：' + LOCAL_BASE)
  }
}

// 上报 short-video worker（同一 plan 全站通用；fail-soft）
async function registerSupporterRemote(subId, rec) {
  try {
    const r = await fetch(SUP_WORKER + '/api/support/register', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'user-agent': 'ppe-room-gateway' },
      body: JSON.stringify({ sub: subId, src: 'ppe', room: rec.room || '' }),
      signal: AbortSignal.timeout(12000),
    }).catch(e => null)
    if (r === null) { console.log('[支持者] 上报网络失败（有私库登记兜底）'); return }
    const j = await r.json().catch(() => ({}))
    console.log('[支持者] 上报 short-video: ' + r.status + ' ' + JSON.stringify(j).slice(0, 120))
  } catch (_) {}
}

/* ------------------------------------------------------------ 聊天记录 */

// 桌面实时记录 + 定时私库归档
async function recordChat(room, str) {
  let m = null
  try { m = JSON.parse(str) } catch (_) { return }
  if (!m || !m.sender) return
  const today = dayStr(Date.now())
  const rec = { t: Date.now(), from: m.sender, room }
  if (m.img != null) {
    rec.img = true
    rec.imgBytes = String(m.img || '').length
    if (m.text) rec.text = String(m.text).slice(0, 500)
    rec.imgData = String(m.img || '')   // 桌面存原 dataUrl；私库归档时转标记
  } else {
    rec.text = String(m.text || '').slice(0, 2000)
  }
  try {
    fs.mkdirSync(CHAT_DIR, { recursive: true })
    fs.appendFileSync(path.join(CHAT_DIR, today + '.jsonl'), JSON.stringify(rec) + '\n', 'utf8')
    chatDirtyDays.add(today)
  } catch (e) {
    console.log('[聊天记录] 写入失败: ' + (e && e.message))
  }
  if (CHAT_LOG) {
    console.log('[聊天] ' + room + ' · ' + m.sender + ': ' + (rec.img ? '[图片]' : String(rec.text || '').slice(0, 120)))
  }
}

// 私库归档：整文件覆盖式同步（今天若有新行 → 全量镜像过去，图片转标记）
let archivingChat = false
async function archiveChat() {
  if (!privateEnabled() || archivingChat || chatDirtyDays.size === 0) return
  archivingChat = true
  try {
    const days = [...chatDirtyDays]
    chatDirtyDays.clear()
    for (const day of days) {
      const p = path.join(CHAT_DIR, day + '.jsonl')
      let content = ''
      let lines = 0
      try {
        content = fs.readFileSync(p, 'utf8')
      } catch (_) { continue }
      const mirrored = content.split(/\r?\n/).filter(Boolean).map((l) => {
        try {
          const o = JSON.parse(l)
          if (o.imgData) { o.img = '[图片 ' + Math.round(o.imgData.length / 1024) + 'KB]'; delete o.imgData }
          return JSON.stringify(o)
        } catch (_) { return l }
      })
      lines = mirrored.length
      const ok = await writePrivateFile('chatlogs/' + day + '.jsonl', mirrored.join('\n') + '\n', '聊天归档 ' + day + '（' + lines + ' 条）')
      if (ok) console.log('[归档] ' + day + ' 已归档（' + lines + ' 行）')
      else if (fs.existsSync(p)) { chatDirtyDays.add(day); console.log('[归档] ' + day + ' 归档未成功（下轮重试）') }
    }
  } finally {
    archivingChat = false
  }
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

  const existed = state.requests.find(x => x.id === r.id)
  if (!existed) {
    state.requests.push({
      id: r.id,
      room,
      name: r.name.slice(0, 20),
      text: String(r.text || '').slice(0, 500),
      t: r.t,
      issue: 0,
      repo: '',
    })
  }
  persistState()

  const item = existed || state.requests[state.requests.length - 1]
  if (existed) return

  console.log('============================================================')
  console.log('[申请]  房间 ' + room + ' · ' + item.name + ' · ' + fmtTime(item.t))
  console.log('[留言]  ' + (item.text || '（无）'))
  console.log('[处理]  面板 ' + LOCAL_BASE + ' ｜ 一键开启 ' + LOCAL_BASE + '/api/open?room=' + room + '&k=' + sign(room, 'open'))
  console.log('============================================================')

  if (isOpen(room)) {
    resolveByRoom(room)
    return
  }
  await notifyGitHub(item)
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
    '> 链接指向本机 127.0.0.1:' + PORT + '，只是在网页里点击无效；',
    '> 拒绝后房间保持关闭，申请者可再次申请；开启后会自动关闭本 Issue。',
    '> 房间开关的「唯一权威」在私库 rooms.json —— 你也可以直接改私库，网关 30 秒内自动应用。',
  ].join('\n')

  const j = await createIssue(PRIVATE_REPO, title, body)
  if (j && j.number) {
    const idx = state.requests.findIndex(x => x.id === item.id)
    if (idx >= 0) {
      state.requests[idx].issue = j.number
      state.requests[idx].repo = PRIVATE_REPO
      persistState()
    }
  }
}

/* ------------------------------------------------------------ MQTT */
const client = mqtt.connect(BROKERS[0], {
  clientId: 'lcz-gw-' + crypto.randomBytes(4).toString('hex'),
  reconnectPeriod: 3000,
  connectTimeout: 15000,
})

client.on('connect', () => {
  mqttUp = true
  console.log('[MQTT] 已连接 ' + BROKERS[0])
  client.subscribe([
    ROOM_PREFIX + '+/request',
    ROOM_PREFIX + '+/control',
    ROOM_PREFIX + '+/chat',
    ROOM_PREFIX + '+/visit',
  ], { qos: 1 }, (err) => {
    console.log(err ? '[MQTT] 订阅失败: ' + err.message : `[MQTT] 已订阅 ${ROOM_PREFIX}<房间>/{request,control,chat,visit}（retained 会自动补收）`)
  })
})

client.on('reconnect', () => { mqttUp = false; console.log('[MQTT] 重连中……') })
client.on('close', () => { mqttUp = false; console.log('[MQTT] 连接断开，稍后自动重连') })
client.on('error', (e) => { mqttUp = false; console.log('[MQTT] 错误: ' + (e && e.message)) })

client.on('message', (topic, payload) => {
  const str = payload.toString()
  if (!str) return
  const parts = topic.split('/')
  if (parts.length !== 3 || parts[0] !== 'succession') return
  const room = sanitizeRoom(parts[1] || '')
  const kind = parts[2]
  if (kind === 'request') {
    handleRequest(room, str).catch(e => console.log('[申请] 处理失败: ' + (e && e.message)))
  } else if (kind === 'control') {
    handleControl(room, str)
  } else if (kind === 'chat') {
    recordChat(room, str)
  } else if (kind === 'visit') {
    handleVisit(room, str)
  }
})

/* ------------------------------------------------------------ HTTP 管理面板 */

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
    '<title>房间管理网关</title><style>body{font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;background:#0f172a;color:#e2e8f0;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}div{max-width:560px;text-align:center}h2{font-size:18px;margin-bottom:10px}p{font-size:13px;color:#94a3b8;line-height:1.8}a{color:#60a5fa}</style></head><body><div><h2>' + title +
    '</h2><p>' + detail + '</p><p><a href="/">← 返回管理面板</a>' + (redirectSec ? '（' + redirectSec + ' 秒后自动返回）' : '') + '</p></div></body></html>'
}

function listData() {
  return {
    now: Date.now(),
    mqtt: mqttUp,
    token: !!GITHUB_TOKEN,
    issues: TEST_NO_ISSUES ? 'off' : 'on',
    privateOk: privateEnabled(),
    privateRepo: PRIVATE_REPO,
    chatArchive: CHAT_DIR,
    key: CONTROL_KEY,
    visits: {
      total: visitTotal,
      issue: (state.visitIssue && state.visitIssue.number) || 0,
      issueUrl: (state.visitIssue && state.visitIssue.url) || '',
      knownRooms: state.knownRooms.length,
      recent: visitRecent.slice(0, 10),
    },
    pending: state.requests.map(r => ({
      id: r.id,
      room: r.room,
      name: r.name,
      text: r.text,
      t: r.t,
      issue: r.issue || 0,
      issueUrl: r.issue ? 'https://github.com/' + (r.repo || PRIVATE_REPO) + '/issues/' + r.issue : '',
      openUrl: LOCAL_BASE + '/api/open?room=' + encodeURIComponent(r.room) + '&k=' + sign(r.room, 'open'),
      denyUrl: LOCAL_BASE + '/api/deny?id=' + encodeURIComponent(r.id) + '&k=' + sign(r.room, 'deny'),
    })),
    open: state.openRooms.slice().sort((a, b) => b.t - a.t).map(r => ({
      room: r.room,
      by: r.by,
      t: r.t,
      paid: r.by === 'paid',
      closeUrl: r.by === 'paid' ? '' : LOCAL_BASE + '/api/close?room=' + encodeURIComponent(r.room) + '&k=' + sign(r.room, 'close'),
    })),
  }
}

function panelHtml() {
  const data = JSON.stringify(CONTROL_KEY)
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
  .paid-flag { color:#fbbf24; font-size:12px }
</style>
</head>
<body>
<header>
  <h1>房间管理网关</h1>
  <span id="stat">加载中……</span>
</header>
<main>
  <section>
    <h2>待处理申请（私库 Issue 通知）</h2>
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
    <span class="tip">
      · 私库 ` + PRIVATE_REPO + ` 的 rooms.json 是开关唯一权威：直接改它，网关 30 秒内自动应用到全站；<br>
      · 「🟡 付费」房是订阅者付费开启的，按你的要求**永远不被关闭**（面板也不给关闭按钮）；<br>
      · 相关所有动作都会记进私库 audit.jsonl（桌面另有 audit-local.jsonl）；<br>
      · 聊天内容实时写入 ` + CHAT_DIR + `，并在有新聊天时自动归档到私库 chatlogs\YYYY-MM-DD.jsonl。
    </span>
  </section>
</main>
<script>
const KEY = ${data};
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
    ' · 令牌 ' + (j.token ? '已配置' : '未配置') +
    (j.issues === 'off' ? ' · Issue 测试模式' : ' · Issue 通知开') +
    ' · 私库 ' + (j.privateOk ? '已连接' : '未连接') +
    ' · 访客 ' + (j.visits ? j.visits.total : 0) +
    ' · 待处理 ' + j.pending.length + ' · 已开 ' + j.open.length;
  const pend = document.getElementById('pend');
  const open2 = document.getElementById('opens');
  if (!j.pending.length) pend.innerHTML = '<div class="empty">暂无申请，有人提交后会在这里出现（私库也会收到 Issue 通知）。</div>';
  else {
    pend.innerHTML = j.pending.map(function(r){
      return '<div class="row">' +
        '<div class="rt">房间 <b>' + esc(r.room) + '</b> · 申请人 ' + esc(r.name) + '</div>' +
        '<div class="rs">' + fmtT(r.t) + (r.issueUrl ? ' · <a style="color:#60a5fa" target="_blank" href="' + esc(r.issueUrl) + '">私库 Issue #' + r.issue + '</a>' : '') + '</div>' +
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
      const by = r.by === 'paid' ? '<span class="paid-flag">🟡 付费开启（不可关闭）</span>' : (r.by === 'admin' ? '管理员开启' : esc(r.by));
      return '<div class="row">' +
        '<div class="rt">房间 <b>' + esc(r.room) + '</b> <span class="rs">· ' + by + '</span></div>' +
        '<div class="rs">' + fmtT(r.t) + '</div>' +
        (r.closeUrl ? '<div class="btns"><button class="b-cl" data-url="' + esc(r.closeUrl) + '" data-confirm="关闭房间「' + esc(r.room) + '」？付费用户可自行恢复开启。">⛔ 关闭房间</button></div>' : '') +
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
    if (k !== sign(room, act)) return sendHtml(res, 403, resultPage('⛔ 校验失败', '这是一个仅限本机的管理接口，请从管理面板或私库 Issue 里的链接点击。', 0))
    if (act === 'open') {
      approveRoom(room, 'panel-link')
      return sendHtml(res, 200, resultPage('✅ 房间「' + room + '」已开启', '已发布开启状态并回写私库权限文件，进房的人现在可以发言了。', 2))
    }
    if (!closeRoom(room, 'panel-link')) {
      return sendHtml(res, 403, resultPage('🟡 付费房间不能关闭', '该房间是订阅者付费开启的。按你的既定规则：付费开启的房间不会被任何人关闭。', 3))
    }
    return sendHtml(res, 200, resultPage('⛔ 房间「' + room + '」已关闭', '房间恢复默认关闭状态，操作已回写私库权限文件。', 2))
  }

  if (p === '/api/deny') {
    const id = String(u.searchParams.get('id') || '')
    const k = String(u.searchParams.get('k') || '')
    const item = state.requests.find(r => r.id === id)
    if (!item) return sendHtml(res, 404, resultPage('ℹ️ 申请不存在或已处理', '这条申请可能已被开启时自动消化，无需再处理。', 3))
    if (k !== sign(item.room, 'deny')) return sendHtml(res, 403, resultPage('⛔ 校验失败', '这是一个仅限本机的管理接口，请从私库 Issue 或管理面板里的链接点击。', 0))
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

if (TEST_NO_ISSUES) console.log('*** 测试模式 LCZ_ISSUES=off：不会创建真实 Issue（两个库都跳过） ***')
if (PRIVATE_DISABLE) {
  console.log('*** LCZ_PRIVATE=off：不读写私库（本地面板/聊天主文件照常） ***')
} else {
  console.log('[私库] 目标仓库 ' + PRIVATE_REPO + ' ｜ 开关轮询 ' + POLL_SEC + 's ｜ 聊天归档 ' + ARCHIVE_SEC + 's → chatlogs/')
}
if (CHAT_LOG) console.log('*** 控制台聊天记录已开启（LCZ_CHATLOG=off 关闭）***')

console.log('[网关] 状态文件: ' + STATE_FILE)
console.log('[网关] 聊天记录桌面目录: ' + CHAT_DIR)
console.log('[网关] GitHub 令牌: ' + (GITHUB_TOKEN ? '已配置（' + ((process.env.GITHUB_TOKEN ? '环境变量' : configFound.from) || 'config.json') + '）' : '未配置'))
console.log('[网关] 支持者上报: ' + SUP_WORKER + '（secrets 未配/网络不通时自动放弃，有私库 supporters.json 兜底）')
console.log('[网关] 待处理申请 ' + state.requests.length + ' 条 · 已开房间 ' + state.openRooms.length + ' 个 · 已处理 id ' + state.processed.length + ' 个')

probePrivate().then(() => {
  return preloadSupporters()
}).then(() => {
  // 私库权威轮询 + 聊天归档定时
  setInterval(() => { pollRooms().catch(e => console.log('[私库] 轮询异常: ' + (e && e.message))) }, POLL_SEC * 1000)
  setInterval(() => { archiveChat().catch(e => console.log('[归档] 异常: ' + (e && e.message))) }, ARCHIVE_SEC * 1000)
  // 访客汇总定时（攒批 → 追加到长期 Issue 评论）
  setInterval(() => { flushVisitBuffer().catch(e => console.log('[访客] 汇总异常: ' + (e && e.message))) }, VISIT_FLUSH_MS)
  // 房号基线（openRooms + 私库 rooms.json）已经存在的不通知；此后房号首次出现才建 Issue
  seedKnownRooms().catch(e => console.log('[新房间] 基线异常: ' + (e && e.message)))
  // 开机先归档一遍历史聊天（把之前没归的补上）
  try {
    if (fs.existsSync(CHAT_DIR)) {
      for (const f of fs.readdirSync(CHAT_DIR)) {
        if (/^\d{4}-\d{2}-\d{2}\.jsonl$/.test(f)) chatDirtyDays.add(f.replace('.jsonl', ''))
      }
    }
  } catch (_) {}
  archiveChat().catch(() => {})
})

server.listen(PORT, '127.0.0.1', () => {
  console.log('[网关] 面板已就绪 → ' + LOCAL_BASE)
  console.log('[网关] 工作流：免费申请 → 私库 Issue 通知你 → 面板/一键链接开启；付费房自动开并永久保持；开关权威在私库 rooms.json')
})
