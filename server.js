// ============================================================================
// Chase Game Server — Stage 1 (chase + rock-paper-scissors slap duel)
// ----------------------------------------------------------------------------
// Serves the client (public/index.html) AND runs the real-time game logic
// over Socket.io, so a single deployment (one URL) is all you need.
// ============================================================================

const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

// ---- Tunable game numbers -------------------------------------------------
const CONFIG = {
  ROUND_TIME_SEC: 90,      // length of stage 1, in seconds
  SLAP_PENALTY: 50,        // points lost when caught & slapped
  ESCAPE_REWARD: 30,       // points gained when you escape the slap (adjust freely)
  DUEL_COOLDOWN_MS: 2000,  // grace period after a duel before another can start
  RPS_TIME_MS: 4000,       // time each player has to pick rock/paper/scissors
  MAX_TIE_RETRIES: 3       // how many times a rock/paper/scissors tie re-rolls
};

const rooms = Object.create(null);   // roomCode -> room state
const leaderboard = [];              // { name, opponent, score, result, at }

function makeRoomCode() {
  let code;
  do {
    code = Math.random().toString(36).slice(2, 6).toUpperCase();
  } while (rooms[code]);
  return code;
}

function otherPlayer(room, socketId) {
  return room.players.find((id) => id !== socketId);
}

function beats(a, b) {
  return (
    (a === 'rock' && b === 'scissors') ||
    (a === 'scissors' && b === 'paper') ||
    (a === 'paper' && b === 'rock')
  );
}

io.on('connection', (socket) => {
  // --- Lobby: create / join -------------------------------------------------
  socket.on('create_room', ({ name }) => {
    const code = makeRoomCode();
    rooms[code] = {
      code,
      players: [socket.id],
      names: { [socket.id]: name || 'بازیکن ۱' },
      characters: {},
      scores: { [socket.id]: 0 },
      positions: {},
      duel: null,
      cooldownUntil: 0,
      timer: null,
      timeLeft: CONFIG.ROUND_TIME_SEC,
      started: false
    };
    socket.join(code);
    socket.data.room = code;
    socket.emit('room_created', { code });
  });

  socket.on('join_room', ({ code, name }) => {
    const room = rooms[(code || '').toUpperCase()];
    if (!room) return socket.emit('error_msg', { message: 'کد اتاق پیدا نشد.' });
    if (room.players.length >= 2) return socket.emit('error_msg', { message: 'این اتاق پره.' });

    room.players.push(socket.id);
    room.scores[socket.id] = 0;
    room.names[socket.id] = name || 'بازیکن ۲';
    socket.join(room.code);
    socket.data.room = room.code;

    io.to(room.code).emit('room_ready', {
      names: room.names
    });
  });

  // --- Character selection ---------------------------------------------------
  socket.on('select_character', ({ character }) => {
    const room = rooms[socket.data.room];
    if (!room) return;
    room.characters[socket.id] = character; // 'man' | 'woman'
    io.to(room.code).emit('character_update', { characters: room.characters });

    if (
      room.players.length === 2 &&
      Object.keys(room.characters).length === 2 &&
      !room.started
    ) {
      startRound(room.code);
    }
  });

  // --- Movement sync -----------------------------------------------------
  socket.on('move', (pos) => {
    const room = rooms[socket.data.room];
    if (!room || !room.started) return;
    room.positions[socket.id] = pos;
    const opp = otherPlayer(room, socket.id);
    if (opp) io.to(opp).emit('opponent_move', pos);
  });

  // --- Catch attempt -> starts a duel -------------------------------------
  socket.on('catch_attempt', () => {
    const room = rooms[socket.data.room];
    if (!room || !room.started || room.duel) return;
    if (Date.now() < room.cooldownUntil) return;

    const opp = otherPlayer(room, socket.id);
    if (!opp) return;

    room.duel = { catcher: socket.id, target: opp, choices: {}, tries: 0, timeout: null };
    io.to(room.code).emit('duel_start', {
      catcher: socket.id,
      target: opp,
      timeMs: CONFIG.RPS_TIME_MS
    });
    room.duel.timeout = setTimeout(() => resolveDuel(room.code), CONFIG.RPS_TIME_MS);
  });

  socket.on('rps_choice', ({ choice }) => {
    const room = rooms[socket.data.room];
    if (!room || !room.duel) return;
    if (![room.duel.catcher, room.duel.target].includes(socket.id)) return;

    room.duel.choices[socket.id] = choice;
    if (Object.keys(room.duel.choices).length === 2) {
      clearTimeout(room.duel.timeout);
      resolveDuel(room.code);
    }
  });

  socket.on('get_leaderboard', () => {
    socket.emit('leaderboard', leaderboard.slice(-20).reverse());
  });

  socket.on('disconnect', () => {
    const room = rooms[socket.data.room];
    if (!room) return;
    io.to(room.code).emit('opponent_left');
    clearInterval(room.timer);
    if (room.duel) clearTimeout(room.duel.timeout);
    delete rooms[room.code];
  });
});

function startRound(code) {
  const room = rooms[code];
  room.started = true;
  room.timeLeft = CONFIG.ROUND_TIME_SEC;

  io.to(code).emit('start_game', { characters: room.characters, names: room.names });

  room.timer = setInterval(() => {
    room.timeLeft -= 1;
    io.to(code).emit('time_update', { timeLeft: room.timeLeft, scores: room.scores });
    if (room.timeLeft <= 0) {
      clearInterval(room.timer);
      endRound(code);
    }
  }, 1000);
}

function resolveDuel(code) {
  const room = rooms[code];
  if (!room || !room.duel) return;

  const { catcher, target, choices } = room.duel;
  const options = ['rock', 'paper', 'scissors'];
  const cChoice = choices[catcher] || options[Math.floor(Math.random() * 3)];
  const tChoice = choices[target] || options[Math.floor(Math.random() * 3)];

  let result; // 'catcher' | 'target' | 'tie'
  if (cChoice === tChoice) {
    room.duel.tries += 1;
    if (room.duel.tries < CONFIG.MAX_TIE_RETRIES) {
      room.duel.choices = {};
      io.to(code).emit('duel_tie_retry', { tries: room.duel.tries });
      room.duel.timeout = setTimeout(() => resolveDuel(code), CONFIG.RPS_TIME_MS);
      return;
    }
    result = 'tie';
  } else if (beats(cChoice, tChoice)) {
    result = 'catcher'; // catcher wins -> slaps target
  } else {
    result = 'target'; // target escaped
  }

  if (result === 'catcher') {
    room.scores[target] -= CONFIG.SLAP_PENALTY;
  } else if (result === 'target') {
    room.scores[target] += CONFIG.ESCAPE_REWARD;
  }

  room.cooldownUntil = Date.now() + CONFIG.DUEL_COOLDOWN_MS;
  io.to(code).emit('duel_result', {
    result,
    catcher,
    target,
    catcherChoice: cChoice,
    targetChoice: tChoice,
    scores: room.scores
  });
  room.duel = null;
}

function endRound(code) {
  const room = rooms[code];
  if (!room) return;

  const [p1, p2] = room.players;
  leaderboard.push({
    p1Name: room.names[p1],
    p1Score: room.scores[p1],
    p2Name: room.names[p2],
    p2Score: room.scores[p2],
    at: Date.now()
  });

  io.to(code).emit('stage_end', { scores: room.scores, names: room.names });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Chase game server running on port ${PORT}`));
