import mqtt from 'mqtt'
import http from 'node:http'
import fs from 'node:fs'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

const BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt',
]
const ROOM_PREFIX = 'succession/'
const PORT = 8899
const DESKTOP = path.join(os.homedir(), 'Desktop')
const ROOT = path.join(DESKTOP, '聊天审核')
const DIRS = {
  pending: path.join(ROOT, '待审'),
  approved: path.join(ROOT, '已通过'),
  rejected: path.join(ROOT, '已拒绝'),
}

for (const d of Object.values(DIRS)) fs.mkdirSync(d, { recursive: true })

const seen = new Set()
let lastRefresh = 0
let currentCache = { pending: [], approved: [], rejected: [] }

function fileName(id) {
  return id.replace(/[^a-zA-Z0-9_-]/g, '_') + '.json'
}
function filePath(kind, id) {
  return path.join(DIRS[kind], fileName(id))
}

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch (_) {
    return null
  }
}

async function listMsgs(kind) {
  try {
    const files = await fsp.readdir(DIRS[kind])
    const out = []
    for (const f of files) {
      if (!f.endsWith('.json')) continue
      const rec = readJson(path.join(DIRS[kind], f))
      if (rec) out.push(rec)
    }
    out.sort((a, b) => (a.t || 0) - (b.t || 0))
    return out
  } catch (_) {
    return []
  }
}

function publishReviewed(msg, room, { approved, rejected } = {}) {
  const topic = ROOM_PREFIX + room + '/' + (rejected ? 'rejected' : 'reviewed')
  let published = 0
  for (const c of clients) {
    if (c.connected) {
      c.publish(topic, JSON.stringify(msg), { qos: 0 })
      published++
    }
  }
  return published
}

let clients = []

function connectAll() {
  clients = BROKERS.map((url) => {
    const c = mqtt.connect(url, {
      clientId: 'reviewgw-' + Math.random().toString(36).slice(2, 10),
      reconnectPeriod: 3000,
      connectTimeout: 15000,
    })
    c.on('connect', () => {
      console.log('已连接: ' + url)
      c.subscribe(ROOM_PREFIX + '+/chat', { qos: 0 })
    })
    c.on('message', (topic, payload) => {
      let m
      try {
        m = JSON.parse(payload.toString())
      } catch (_) {
        return
      }
      if (!m || !m.id || seen.has(m.id)) return
      seen.add(m.id)
      const parts = topic.split('/')
      const room = parts[1] || 'global'
      const record = {
        id: m.id,
        room,
        sender: m.sender,
        t: m.t,
        await: Date.now(),
        msg: m,
      }
      try {
        fs.writeFileSync(filePath('pending', m.id), JSON.stringify(record, null, 2))
      } catch (_) {}
      console.log(`[待审] ${room} ${m.sender}: ${textPreview(m)}`)
    })
    c.on('error', () => {})
    c.on('close', () => console.log('断开: ' + url))
    return c
  })
}

function textPreview(m) {
  if (m.text != null) return ('' + m.text).slice(0, 60)
  if (m.img) return '[图片]'
  if (m.data) return '[加密消息]'
  return '[未知]'
}

function reasonFor(m) {
  if (m.text != null) return '' + m.text
  if (m.img) return '图片'
  if (m.data) return '[加密消息]'
  return ''
}

function serve() {
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1:' + PORT)
    if (url.pathname === '/' || url.pathname === '/index.html') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
      res.end(html)
      return
    }
    if (url.pathname === '/api/list') {
      pendingTouch()
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' })
      res.end(JSON.stringify(currentCache))
      return
    }
    if (url.pathname === '/api/approve' || url.pathname === '/api/reject') {
      const id = url.searchParams.get('id')
      if (!id) {
        res.writeHead(400)
        res.end('missing id')
        return
      }
      const approve = url.pathname.endsWith('/approve')
      const from = filePath('pending', id)
      const src = readJson(from)
      let room = 'global'
      if (src) {
        room = src.room || 'global'
        const to = approve ? DIRS.approved : DIRS.rejected
        try {
          fs.renameSync(from, path.join(to, fileName(id)))
        } catch (_) {}
        if (approve) publishReviewed(src.msg, room, { approved: true })
        else publishReviewed(src.msg, room, { rejected: true })
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' })
      res.end(JSON.stringify({ ok: true, approve, id, room }))
      return
    }
    if (url.pathname === '/api/remove') {
      const id = url.searchParams.get('id')
      const kind = url.searchParams.get('kind')
      if (id && DIRS[kind]) {
        try { fs.unlinkSync(filePath(kind, id)) } catch (_) {}
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' })
      res.end(JSON.stringify({ ok: true }))
      return
    }
    res.writeHead(404)
    res.end('not found')
  })
  server.listen(PORT, '127.0.0.1', () => {
    console.log('')
    console.log('=== 群聊审核网关 ===')
    console.log('待审目录: ' + DIRS.pending)
    console.log('审核面板: http://127.0.0.1:' + PORT)
    console.log('聊天页只显示“已通过”的消息')
  })
}

async function pendingTouch() {
  const now = Date.now()
  if (now - lastRefresh < 600) return
  lastRefresh = now
  currentCache = {
    pending: await listMsgs('pending'),
    approved: await listMsgs('approved'),
    rejected: await listMsgs('rejected'),
  }
}

const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>群聊审核面板</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,"Segoe UI","Microsoft YaHei",sans-serif;background:#f4f6fb;color:#1f2430;padding:24px}
h1{font-size:20px;margin-bottom:16px}
.card{background:#fff;border-radius:12px;box-shadow:0 6px 24px rgba(31,41,55,.08);padding:16px;margin-bottom:16px}
.row{display:flex;gap:8px;justify-content:space-between;align-items:center}
.meta{color:#8a93a6;font-size:13px;margin-bottom:6px}
.room-tag{display:inline-block;background:#eff6ff;color:#2563eb;border-radius:6px;padding:0 8px;font-size:12px}
.content{font-size:15px;line-height:1.5;word-break:break-word;margin-bottom:10px}
img.preview{max-width:220px;border-radius:8px;display:block;margin:8px 0}
.btn{border:none;border-radius:8px;padding:7px 16px;font-size:14px;font-weight:600;cursor:pointer}
.btn-ok{background:#16a34a;color:#fff}.btn-ok:hover{background:#15803d}
.btn-no{background:#ef4444;color:#fff}.btn-no:hover{background:#dc2626}
.tabs{display:flex;gap:8px;margin-bottom:16px}
.tab{background:#fff;border:1px solid #e5e9f2;border-radius:8px;padding:8px 16px;cursor:pointer;font-size:14px}
.tab.on{background:#3b82f6;color:#fff;border-color:#3b82f6}
.hidden{display:none}
.empty{color:#8a93a6;font-size:14px;padding:16px}
.fresh{animation:flash 1s ease}
@keyframes flash{0%{background:#fff7ed}100%{background:#fff}}
</style>
</head>
<body>
<h1>群聊审核面板</h1>
<div class="tabs">
  <button class="tab on" data-kind="pending">待审核 <span id="c0"></span></button>
  <button class="tab" data-kind="approved">已通过 <span id="c1"></span></button>
  <button class="tab" data-kind="rejected">已拒绝 <span id="c2"></span></button>
</div>
<div id="lists"></div>
<script>
const $=s=>document.querySelector(s)
let kind='pending'
const KIND_NAME={pending:'待审核',approved:'已通过',rejected:'已拒绝'}
function el(tag,cls,text){const e=document.createElement(tag);if(cls)e.className=cls;if(text!=null)e.textContent=text;return e}
document.querySelectorAll('.tab').forEach(t=>t.addEventListener('click',()=>{
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('on'))
  t.classList.add('on');kind=t.dataset.kind;render()
}))
function itemHTML(rec){
  const card=el('div','card')
  const row=el('div','row')
  const left=el('div')
  const meta=el('div','meta',(rec.sender||'?')+' · '+new Date(rec.t).toLocaleString('zh-CN'))
  left.appendChild(meta)
  const room=el('span','room-tag',rec.room||'global')
  meta.appendChild(document.createTextNode(' '));left.appendChild(room)
  row.appendChild(left)
  const btns=el('div')
  if(kind==='pending'){
    const ok=el('button','btn btn-ok','通过')
    ok.addEventListener('click',()=>act(rec.id,true))
    const no=el('button','btn btn-no','拒绝')
    no.addEventListener('click',()=>act(rec.id,false))
    ok.style.marginRight='8px';no.style.marginRight='8px'
    btns.appendChild(ok);btns.appendChild(no)
  }
  const del=el('button','btn btn-no','删除')
  del.addEventListener('click',async()=>{await fetch('/api/remove?id='+encodeURIComponent(rec.id)+'&kind='+kind);render()})
  del.style.background='#6b7280'
  btns.appendChild(del)
  row.appendChild(btns)
  card.appendChild(row)
  const c=el('div','content')
  if(rec.msg){
    if(rec.msg.img!=null){
      const img=document.createElement('img')
      img.className='preview';img.src=rec.msg.img;img.alt='图片'
      c.appendChild(img)
    }else if(rec.msg.text!=null){
      c.textContent=rec.msg.text
    }else if(rec.msg.data){
      c.textContent='[加密消息 · 房间口令加密，审核员无法查看内容]'
    }else c.textContent='[未知]'
  }
  card.appendChild(c)
  return card
}
function rejectStatic(id){
  const s=el('span','fresh','');s.className='';s.textContent=''
  return s
}
async function act(id,isOk){
  const res=await fetch('/api/'+(isOk?'approve':'reject')+'?id='+encodeURIComponent(id))
  const j=await res.json()
  if(j.ok)render()
}
async function render(){
  const res=await fetch('/api/list')
  const data=await res.json()
  $('#c0').textContent=data.pending.length
  $('#c1').textContent=data.approved.length
  $('#c2').textContent=data.rejected.length
  const lists=$('#lists');lists.textContent=''
  const arr=data[kind]
  if(!arr.length){lists.appendChild(el('div','empty','暂无'+KIND_NAME[kind]+'的消息'))}
  for(const rec of arr){
    const node=itemHTML(rec)
    if(kind==='pending')node.classList.add('fresh')
    lists.appendChild(node)
  }
}
setInterval(render,2000)
render()
</script>
</body>
</html>`

console.log('正在启动审核网关……')
connectAll()
serve()