const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); res.end('Error'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });

let queue = [];           // sockets waiting for a match
const rooms = new Map();  // roomId -> { host, guest, state }
let nextRoom = 1;

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function roomOf(ws) {
  for (const [id, room] of rooms) {
    if (room.host === ws || room.guest === ws) return { id, room };
  }
  return null;
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join_queue') {
      // Remove from any existing queue slot
      queue = queue.filter(s => s !== ws);
      queue.push(ws);
      send(ws, { type: 'queued', position: queue.length });
      console.log(`Player queued. Queue size: ${queue.length}`);

      // Match two players if we have enough
      if (queue.length >= 2) {
        const host = queue.shift();
        const guest = queue.shift();
        const roomId = nextRoom++;
        rooms.set(roomId, {
          host, guest,
          hostReady: false, guestReady: false,
          hostLobby: {}, guestLobby: {}
        });
        host._roomId = roomId;
        guest._roomId = roomId;
        send(host, { type: 'matched', role: 'host', roomId });
        send(guest, { type: 'matched', role: 'guest', roomId });
        console.log(`Room ${roomId} created`);
      }
    }

    if (msg.type === 'lobby_update') {
      const r = roomOf(ws);
      if (!r) return;
      const { room } = r;
      const isHost = ws === room.host;
      if (isHost) {
        room.hostLobby = msg.data;
        send(room.guest, { type: 'opponent_lobby', data: msg.data });
      } else {
        room.guestLobby = msg.data;
        send(room.host, { type: 'opponent_lobby', data: msg.data });
      }
    }

    if (msg.type === 'set_ready') {
      const r = roomOf(ws);
      if (!r) return;
      const { room } = r;
      if (ws === room.host) room.hostReady = msg.ready;
      else room.guestReady = msg.ready;

      // Broadcast ready states to both
      send(room.host, { type: 'ready_state', hostReady: room.hostReady, guestReady: room.guestReady });
      send(room.guest, { type: 'ready_state', hostReady: room.hostReady, guestReady: room.guestReady });

      // Both ready → start
      if (room.hostReady && room.guestReady) {
        const cfg = {
          type: 'start_game',
          mapId: room.hostLobby.mapId || 'open',
          p1Class: room.hostLobby.classId || 'sniper',
          p1Perks: room.hostLobby.perks || ['none', 'none'],
          p2Class: room.guestLobby.classId || 'shotgunner',
          p2Perks: room.guestLobby.perks || ['none', 'none'],
        };
        send(room.host, cfg);
        send(room.guest, cfg);
        console.log(`Room ${r.id} game starting`);
      }
    }

    // In-game: relay between host and guest
    if (msg.type === 'state') {
      const r = roomOf(ws);
      if (r && ws === r.room.host) send(r.room.guest, msg);
    }

    if (msg.type === 'keys') {
      const r = roomOf(ws);
      if (r && ws === r.room.guest) send(r.room.host, msg);
    }

    if (msg.type === 'end_game') {
      const r = roomOf(ws);
      if (r && ws === r.room.host) send(r.room.guest, msg);
    }
  });

  ws.on('close', () => {
    queue = queue.filter(s => s !== ws);
    const r = roomOf(ws);
    if (r) {
      const other = ws === r.room.host ? r.room.guest : r.room.host;
      send(other, { type: 'opponent_disconnected' });
      rooms.delete(r.id);
      console.log(`Room ${r.id} closed`);
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server on http://0.0.0.0:${PORT} — share your LAN IP with opponents`);
});
