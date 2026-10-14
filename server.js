// Minimal signaling server. Run: npm install ws && node server.js
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url.startsWith('/?')) {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    return fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server });
const receivers = new Map(); // code -> { ws, sender }
const genCode = () => { let c; do c = String(Math.floor(100000 + Math.random() * 900000)); while (receivers.has(c)); return c; };
const send = (ws, o) => ws && ws.readyState === 1 && ws.send(JSON.stringify(o));

wss.on('connection', ws => {
  ws.alive = true; ws.on('pong', () => ws.alive = true);
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'register') {                       // receiver
      const code = genCode(); ws.role = 'rx'; ws.code = code;
      receivers.set(code, { ws, sender: null, name: String(m.name || 'TV').slice(0, 40) });
      send(ws, { t: 'registered', code });
    } else if (m.t === 'join') {                    // sender
      const r = receivers.get(String(m.code));
      if (!r) return send(ws, { t: 'error', msg: 'Invalid or expired pairing code.' });
      if (r.sender && r.sender !== ws && r.sender.readyState === 1) return send(ws, { t: 'error', msg: 'TV already has a connected device.' });
      r.sender = ws; ws.role = 'tx'; ws.code = m.code;
      send(ws, { t: 'joined', tvName: r.name });
      send(r.ws, { t: 'peer-joined', name: String(m.name || 'iPhone').slice(0, 40) });
    } else if (m.t === 'sig') {                     // relay offer/answer/ice
      const r = receivers.get(ws.code); if (!r) return;
      send(ws.role === 'rx' ? r.sender : r.ws, { t: 'sig', data: m.data });
    } else if (m.t === 'leave') cleanup(ws);
  });
  ws.on('close', () => cleanup(ws));
});

function cleanup(ws) {
  const r = receivers.get(ws.code); if (!r) return;
  if (ws.role === 'rx') { send(r.sender, { t: 'peer-left' }); receivers.delete(ws.code); }
  else if (r.sender === ws) { r.sender = null; send(r.ws, { t: 'peer-left' }); }
}
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 15000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('WiFi Screen Mirror running. Open on devices on the same Wi-Fi:');
  for (const l of Object.values(os.networkInterfaces()).flat())
    if (l.family === 'IPv4' && !l.internal) console.log(`  http://${l.address}:${PORT}`);
});
