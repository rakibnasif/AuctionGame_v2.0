/* BidArena server — static site + zero-dependency WebSocket room relay.
   The game itself is host-authoritative in the browser; this server only
   forwards messages between the people in the same room (game state + WebRTC voice signalling). */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');

const PORT = +process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_MSG = 256 * 1024;          // largest accepted message
const MAX_ROOMS = 1000;
const MAX_PER_ROOM = 16;
const MAX_PER_IP = 40;
const LEAVE_GRACE_MS = 4000;         // short network blips are not treated as leaving
const ROOM_RE = /^[A-Z0-9]{5}$/;

/* ---------- ICE (STUN / TURN) config for voice ---------- */
function iceServers() {
  if (process.env.ICE_SERVERS) { try { return JSON.parse(process.env.ICE_SERVERS); } catch (e) { console.error('ICE_SERVERS is not valid JSON'); } }
  const list = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
  if (process.env.TURN_URL) {
    list.push({ urls: process.env.TURN_URL.split(',').map(s => s.trim()), username: process.env.TURN_USERNAME || '', credential: process.env.TURN_CREDENTIAL || '' });
  }
  return list;
}

/* ---------- static files ---------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.svg': 'image/svg+xml', '.webmanifest': 'application/manifest+json', '.png': 'image/png', '.txt': 'text/plain; charset=utf-8', '.ico': 'image/x-icon' };
const cache = new Map();
function loadFile(rel) {
  const full = path.normalize(path.join(PUBLIC, rel));
  if (!full.startsWith(PUBLIC + path.sep) && full !== PUBLIC) return null;
  let st; try { st = fs.statSync(full); } catch (e) { return null; }
  if (!st.isFile()) return null;
  const hit = cache.get(full);
  if (hit && hit.mtime === st.mtimeMs) return hit;
  const body = fs.readFileSync(full), ext = path.extname(full).toLowerCase();
  const item = { mtime: st.mtimeMs, body, gz: /^\.(html|svg|webmanifest|txt)$/.test(ext) ? zlib.gzipSync(body, { level: 9 }) : null, type: MIME[ext] || 'application/octet-stream' };
  cache.set(full, item); return item;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const headers = { 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer', 'Permissions-Policy': 'microphone=(self), camera=()' };
  if (url.pathname === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain', ...headers }); return res.end('ok'); }
  if (url.pathname === '/ice') { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...headers }); return res.end(JSON.stringify({ iceServers: iceServers() })); }
  if (url.pathname === '/stats') { res.writeHead(200, { 'Content-Type': 'application/json', ...headers }); return res.end(JSON.stringify({ rooms: rooms.size, clients: [...rooms.values()].reduce((a, r) => a + r.size, 0) })); }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405, headers); return res.end(); }
  let rel = decodeURIComponent(url.pathname);
  if (rel === '/' || rel === '') rel = '/index.html';
  const f = loadFile(rel.replace(/^\/+/, ''));
  if (!f) { res.writeHead(404, { 'Content-Type': 'text/plain', ...headers }); return res.end('Not found'); }
  const h = { 'Content-Type': f.type, 'Cache-Control': rel === '/index.html' ? 'no-cache' : 'public, max-age=3600', Vary: 'Accept-Encoding', ...headers };
  const useGz = f.gz && /\bgzip\b/.test(req.headers['accept-encoding'] || '');
  const body = useGz ? f.gz : f.body;
  if (useGz) h['Content-Encoding'] = 'gzip';
  h['Content-Length'] = body.length;
  res.writeHead(200, h);
  res.end(req.method === 'HEAD' ? undefined : body);
});

/* ---------- minimal RFC 6455 WebSocket ---------- */
const rooms = new Map();             // code -> Set<client>
const perIp = new Map();             // ip -> count
const pendingLeave = new Map();      // room|cid -> timer

function frame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
  return Buffer.concat([head, payload]);
}
function sendText(c, str) { if (c.dead || !c.socket.writable) return; try { c.socket.write(frame(0x1, Buffer.from(str))); } catch (e) { drop(c); } }
function closeWith(c, code, why) {
  if (c.dead) return;
  const p = Buffer.alloc(2 + Buffer.byteLength(why || '')); p.writeUInt16BE(code, 0); p.write(why || '', 2);
  try { c.socket.write(frame(0x8, p)); } catch (e) {}
  setTimeout(() => drop(c), 200);
}
function ipOf(req) { return ((req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress || '?'; }

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://x');
  const key = req.headers['sec-websocket-key'];
  const room = (url.searchParams.get('room') || '').toUpperCase();
  const ip = ipOf(req);
  const refuse = (code, msg) => { socket.write(`HTTP/1.1 ${code} ${msg}\r\nConnection: close\r\n\r\n`); socket.destroy(); };
  if (url.pathname !== '/ws' || !key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') return refuse(400, 'Bad Request');
  if (!ROOM_RE.test(room)) return refuse(400, 'Bad Room');
  if ((perIp.get(ip) || 0) >= MAX_PER_IP) return refuse(429, 'Too Many Connections');
  let set = rooms.get(room);
  if (!set && rooms.size >= MAX_ROOMS) return refuse(503, 'Server Busy');
  if (set && set.size >= MAX_PER_ROOM) return refuse(403, 'Room Full');
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write(['HTTP/1.1 101 Switching Protocols', 'Upgrade: websocket', 'Connection: Upgrade', 'Sec-WebSocket-Accept: ' + accept, '', ''].join('\r\n'));
  socket.setNoDelay(true); socket.setTimeout(0);
  const c = { socket, room, ip, cid: null, buf: Buffer.alloc(0), frag: [], fragLen: 0, alive: true, dead: false, tokens: 200, last: Date.now() };
  perIp.set(ip, (perIp.get(ip) || 0) + 1);
  if (!set) { set = new Set(); rooms.set(room, set); }
  const peers = set.size;
  set.add(c);
  sendText(c, JSON.stringify({ t: '_welcome', peers }));
  socket.on('data', chunk => { try { onData(c, chunk); } catch (e) { closeWith(c, 1011, 'error'); } });
  socket.on('close', () => drop(c));
  socket.on('error', () => drop(c));
});

function onData(c, chunk) {
  c.buf = Buffer.concat([c.buf, chunk]);
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2) > 0) return closeWith(c, 1009, 'too big'); len = b.readUInt32BE(6); off = 10; }
    if (len > MAX_MSG) return closeWith(c, 1009, 'too big');
    if (!masked) return closeWith(c, 1002, 'unmasked');
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4), payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    if (op === 0x8) { closeWith(c, 1000, ''); return; }
    if (op === 0x9) { try { c.socket.write(frame(0xA, payload)); } catch (e) {} continue; }
    if (op === 0xA) { c.alive = true; continue; }
    if (op === 0x1 || op === 0x0) {
      if (op === 0x1) { c.frag = []; c.fragLen = 0; }
      c.frag.push(payload); c.fragLen += payload.length;
      if (c.fragLen > MAX_MSG) return closeWith(c, 1009, 'too big');
      if (fin) { const text = Buffer.concat(c.frag).toString('utf8'); c.frag = []; c.fragLen = 0; onMessage(c, text); }
    }
    /* binary frames are ignored */
  }
}

function onMessage(c, text) {
  const now = Date.now();
  c.tokens = Math.min(200, c.tokens + (now - c.last) * 0.1); c.last = now;   // 100 msgs/sec sustained
  if (c.tokens < 1) return;
  c.tokens -= 1;
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object' || typeof m.from !== 'string' || m.from.length > 40) return;
  if (!c.cid) {
    c.cid = m.from;
    const t = pendingLeave.get(c.room + '|' + c.cid);
    if (t) { clearTimeout(t); pendingLeave.delete(c.room + '|' + c.cid); }
  } else if (m.from !== c.cid) return;     // no impersonation
  const set = rooms.get(c.room); if (!set) return;
  let targeted = false;
  if (typeof m.to === 'string') {
    for (const o of set) if (o !== c && o.cid === m.to) { sendText(o, text); targeted = true; }
    if (targeted) return;
  }
  for (const o of set) if (o !== c) sendText(o, text);
}

function drop(c) {
  if (c.dead) return;
  c.dead = true;
  try { c.socket.destroy(); } catch (e) {}
  perIp.set(c.ip, Math.max(0, (perIp.get(c.ip) || 1) - 1));
  const set = rooms.get(c.room);
  if (!set) return;
  set.delete(c);
  if (!set.size) rooms.delete(c.room);
  if (c.cid) {
    const k = c.room + '|' + c.cid;
    clearTimeout(pendingLeave.get(k));
    pendingLeave.set(k, setTimeout(() => {
      pendingLeave.delete(k);
      const s = rooms.get(c.room); if (!s) return;
      if ([...s].some(o => o.cid === c.cid)) return;       // reconnected
      const msg = JSON.stringify({ t: 'leave', room: c.room, from: c.cid, srv: 1 });
      for (const o of s) sendText(o, msg);
    }, LEAVE_GRACE_MS));
  }
}

/* keep connections alive through proxies and drop dead ones */
setInterval(() => {
  for (const set of rooms.values()) for (const c of [...set]) {
    if (!c.alive) { drop(c); continue; }
    c.alive = false;
    try { c.socket.write(frame(0x9, Buffer.alloc(0))); } catch (e) { drop(c); }
  }
}, 25000).unref();

if (require.main === module) {
  server.listen(PORT, '0.0.0.0', () => console.log(`BidArena running on http://localhost:${PORT}`));
}
module.exports = { server };
