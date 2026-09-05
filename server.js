// ============================================================================
// Chase Game Server — full 4-stage flow, with reconnect tolerance
// ----------------------------------------------------------------------------
// Players are identified by a persistent `playerId` generated once in the
// client (not by socket.id, which changes on every reconnect). If a socket
// drops — a locked phone, backgrounding the app to open the Telegram share
// sheet, a flaky network — we give it a grace window to reconnect and
// "rejoin" before we tell the room the player actually left.
// ============================================================================

const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  pingInterval: 20000,
  pingTimeout: 40000 // tolerate a backgrounded WebView for a while before Socket.io gives up
});

app.use(express.static(path.join(__dirname, 'public')));

// ---- Tunable game numbers -------------------------------------------------
const CONFIG = {
  ROUND_TIME_SEC: 90,
  SLAP_PENALTY: 50,
  ESCAPE_REWARD: 30,
  DUEL_COOLDOWN_MS: 2000,
  RPS_TIME_MS: 4000,
  MAX_TIE_RETRIES: 3,
  STAGE1_MAX_DUELS: 3, // stage 1 ends immediately once this many duels have been resolved
  STAGE_TRANSITION_MS: 3000,
  STAGE3_BUMP_SLOW_MS: 1200,
  STAGE3_BUMP_COOLDOWN_MS: 1500,
  STAGE3_FINISH_GRACE_SEC: 20,
  RACE_WIN_POINTS: 100,
  RECONNECT_GRACE_MS: 25000 // how long a dropped player has to come back
};

const rooms = Object.create(null);
const leaderboard = [];

function makeRoomCode() {
  let code;
  do {
    code = Math.random().toString(36).slice(2, 6).toUpperCase();
  } while (rooms[code]);
  return code;
}

function otherPlayer(room, playerId) {
  return room.players.find((id) => id !== playerId);
}

function emitToPlayer(room, playerId, event, payload) {
  const sid = room.socketOf[playerId];
  if (sid) io.to(sid).emit(event, payload);
}

function beats(a, b) {
  return (
    (a === 'rock' && b === 'scissors') ||
    (a === 'scissors' && b === 'paper') ||
    (a === 'paper' && b === 'rock')
  );
}

function currentStateSnapshot(room) {
  return {
    stage: room.stage,
    scores: room.scores,
    names: room.names,
    characters: room.characters,
    timeLeft: room.timeLeft,
    trackSeed: room.trackSeed || null
  };
}

io.on('connection', (socket) => {
  // --- Lobby: create / join -------------------------------------------------
  socket.on('create_room', ({ name, playerId }) => {
    if (!playerId) return;
    const code = makeRoomCode();
    rooms[code] = {
      code,
      stage: 0,
      players: [playerId],
      socketOf: { [playerId]: socket.id },
      disconnectTimers: {},
      names: { [playerId]: name || 'بازیکن ۱' },
      characters: {},
      scores: { [playerId]: 0 },
      positions: {},
      duel: null,
      duelCount: 0,
      cooldownUntil: 0,
      timer: null,
      timeLeft: CONFIG.ROUND_TIME_SEC,
      started: false,
      bumpCooldown: {}
    };
    socket.join(code);
    socket.data.room = code;
    socket.data.playerId = playerId;
    socket.emit('room_created', { code });
  });

  socket.on('join_room', ({ code, name, playerId }) => {
    if (!playerId) return;
    const room = rooms[(code || '').toUpperCase()];
    if (!room) return socket.emit('error_msg', { message: 'کد اتاق پیدا نشد.' });
    if (room.players.includes(playerId)) {
      // Same browser session re-submitting join (e.g. double tap) — treat as rejoin.
      return handleRejoin(socket, room.code, playerId);
    }
    if (room.players.length >= 2) return socket.emit('error_msg', { message: 'این اتاق پره.' });

    room.players.push(playerId);
    room.socketOf[playerId] = socket.id;
    room.scores[playerId] = 0;
    room.names[playerId] = name || 'بازیکن ۲';
    socket.join(room.code);
    socket.data.room = room.code;
    socket.data.playerId = playerId;

    io.to(room.code).emit('room_ready', { names: room.names });
  });

  // --- Reconnect: same tab lost its socket and got a new one ---------------
  socket.on('rejoin', ({ code, playerId }) => handleRejoin(socket, code, playerId));

  function handleRejoin(sock, code, playerId) {
    const room = rooms[(code || '').toUpperCase()];
    if (!room || !playerId || !room.players.includes(playerId)) {
      sock.emit('rejoin_failed');
      return;
    }
    clearTimeout(room.disconnectTimers[playerId]);
    delete room.disconnectTimers[playerId];

    room.socketOf[playerId] = sock.id;
    sock.join(room.code);
    sock.data.room = room.code;
    sock.data.playerId = playerId;

    sock.emit('resync', currentStateSnapshot(room));
    const opp = otherPlayer(room, playerId);
    if (opp) emitToPlayer(room, opp, 'opponent_reconnected', {});
  }

  // --- Character selection ---------------------------------------------------
  socket.on('select_character', ({ character }) => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !playerId) return;
    room.characters[playerId] = character; // 'man' | 'woman'
    io.to(room.code).emit('character_update', { characters: room.characters });

    if (
      room.players.length === 2 &&
      Object.keys(room.characters).length === 2 &&
      !room.started
    ) {
      startStage1(room.code);
    }
  });

  // === STAGE 1: chase + slap duel ==========================================
  socket.on('move', (pos) => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !playerId || room.stage !== 1) return;
    room.positions[playerId] = pos;
    const opp = otherPlayer(room, playerId);
    if (opp) emitToPlayer(room, opp, 'opponent_move', pos);
  });

  socket.on('catch_attempt', () => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !playerId || room.stage !== 1 || room.duel) return;
    if (Date.now() < room.cooldownUntil) return;

    const opp = otherPlayer(room, playerId);
    if (!opp) return;

    room.duel = { catcher: playerId, target: opp, choices: {}, tries: 0, timeout: null };
    io.to(room.code).emit('duel_start', {
      catcher: playerId,
      target: opp,
      timeMs: CONFIG.RPS_TIME_MS
    });
    room.duel.timeout = setTimeout(() => resolveDuel(room.code), CONFIG.RPS_TIME_MS);
  });

  socket.on('rps_choice', ({ choice }) => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !room.duel || !playerId) return;
    if (![room.duel.catcher, room.duel.target].includes(playerId)) return;

    room.duel.choices[playerId] = choice;
    if (Object.keys(room.duel.choices).length === 2) {
      clearTimeout(room.duel.timeout);
      resolveDuel(room.code);
    }
  });

  // === STAGE 2: gold mining (client-scored) ===============================
  socket.on('stage2_result', ({ score }) => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !playerId || room.stage !== 2) return;
    room.scores[playerId] = (room.scores[playerId] || 0) + (Number(score) || 0);
    room.stage2Reported.add(playerId);
    if (room.stage2Reported.size === room.players.length) {
      advanceToStage3(room.code);
    }
  });

  // === STAGE 3: racing =====================================================
  socket.on('stage3_progress', (pos) => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !playerId || room.stage !== 3) return;
    const opp = otherPlayer(room, playerId);
    if (opp) emitToPlayer(room, opp, 'opponent_stage3_progress', pos);
  });

  socket.on('stage3_bump', () => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !playerId || room.stage !== 3) return;
    const now = Date.now();
    if (room.bumpCooldown[playerId] && now < room.bumpCooldown[playerId]) return;
    room.bumpCooldown[playerId] = now + CONFIG.STAGE3_BUMP_COOLDOWN_MS;
    const opp = otherPlayer(room, playerId);
    if (opp) emitToPlayer(room, opp, 'you_got_bumped', { slowMs: CONFIG.STAGE3_BUMP_SLOW_MS });
  });

  socket.on('stage3_finish', () => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !playerId || room.stage !== 3) return;
    if (room.stage3Finished.has(playerId)) return;
    room.stage3Finished.add(playerId);

    if (room.stage3Finished.size === 1) {
      room.scores[playerId] = (room.scores[playerId] || 0) + CONFIG.RACE_WIN_POINTS;
      io.to(room.code).emit('stage3_winner', { winner: playerId, scores: room.scores });
    }

    if (room.stage3Finished.size === room.players.length) {
      advanceToStage4(room.code);
    } else {
      clearTimeout(room.stage3Grace);
      room.stage3Grace = setTimeout(
        () => advanceToStage4(room.code),
        CONFIG.STAGE3_FINISH_GRACE_SEC * 1000
      );
    }
  });

  // === STAGE 4: dreamland climb (client-scored) ===========================
  socket.on('stage4_result', ({ score }) => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !playerId || room.stage !== 4) return;
    room.scores[playerId] = (room.scores[playerId] || 0) + (Number(score) || 0);
    room.stage4Reported.add(playerId);
    if (room.stage4Reported.size === room.players.length) {
      finishGame(room.code);
    }
  });

  // --- Leaderboard ----------------------------------------------------------
  socket.on('get_leaderboard', () => {
    socket.emit('leaderboard', leaderboard.slice(-20).reverse());
  });

  // --- Disconnect: grace period before we treat the player as truly gone ---
  socket.on('disconnect', () => {
    const room = rooms[socket.data.room];
    const playerId = socket.data.playerId;
    if (!room || !playerId) return;
    if (room.socketOf[playerId] !== socket.id) return; // a newer socket already took over

    delete room.socketOf[playerId];
    const opp = otherPlayer(room, playerId);
    if (opp) emitToPlayer(room, opp, 'opponent_connection_issue', {});

    room.disconnectTimers[playerId] = setTimeout(() => {
      io.to(room.code).emit('opponent_left');
      clearInterval(room.timer);
      clearTimeout(room.stage3Grace);
      if (room.duel) clearTimeout(room.duel.timeout);
      delete rooms[room.code];
    }, CONFIG.RECONNECT_GRACE_MS);
  });
});

// ============================================================================
// Stage logic
// ============================================================================
function startStage1(code) {
  const room = rooms[code];
  room.stage = 1;
  room.started = true;
  room.timeLeft = CONFIG.ROUND_TIME_SEC;

  io.to(code).emit('start_game', { characters: room.characters, names: room.names });

  room.timer = setInterval(() => {
    room.timeLeft -= 1;
    io.to(code).emit('time_update', { timeLeft: room.timeLeft, scores: room.scores });
    if (room.timeLeft <= 0) {
      clearInterval(room.timer);
      endStage1(code);
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

  let result;
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
    result = 'catcher';
  } else {
    result = 'target';
  }

  if (result === 'catcher') {
    room.scores[target] -= CONFIG.SLAP_PENALTY;
  } else if (result === 'target') {
    room.scores[target] += CONFIG.ESCAPE_REWARD;
  }

  room.cooldownUntil = Date.now() + CONFIG.DUEL_COOLDOWN_MS;
  room.duelCount = (room.duelCount || 0) + 1;
  io.to(code).emit('duel_result', {
    result,
    catcher,
    target,
    catcherChoice: cChoice,
    targetChoice: tChoice,
    scores: room.scores,
    duelsPlayed: room.duelCount,
    maxDuels: CONFIG.STAGE1_MAX_DUELS
  });
  room.duel = null;

  if (room.stage === 1 && room.duelCount >= CONFIG.STAGE1_MAX_DUELS) {
    clearInterval(room.timer);
    endStage1(code);
  }
}

function endStage1(code) {
  const room = rooms[code];
  if (!room || room.stage !== 1) return;
  clearInterval(room.timer);
  room.stage = 0;
  io.to(code).emit('stage1_end', { scores: room.scores, names: room.names });
  setTimeout(() => startStage2(code), CONFIG.STAGE_TRANSITION_MS);
}

function startStage2(code) {
  const room = rooms[code];
  if (!room) return;
  room.stage = 2;
  room.stage2Reported = new Set();
  io.to(code).emit('stage2_start', {});
}

function advanceToStage3(code) {
  const room = rooms[code];
  if (!room || room.stage !== 2) return;
  room.stage = 0;
  io.to(code).emit('stage2_end', { scores: room.scores, names: room.names });
  const trackSeed = Math.floor(Math.random() * 1e9);
  room.trackSeed = trackSeed;
  setTimeout(() => {
    if (!rooms[code]) return;
    room.stage = 3;
    room.stage3Finished = new Set();
    io.to(code).emit('stage3_start', { trackSeed, scores: room.scores });
  }, CONFIG.STAGE_TRANSITION_MS);
}

function advanceToStage4(code) {
  const room = rooms[code];
  if (!room || room.stage === 4 || room.stage === 0) return;
  clearTimeout(room.stage3Grace);
  room.stage = 0;
  io.to(code).emit('stage3_end', { scores: room.scores, names: room.names });
  setTimeout(() => {
    if (!rooms[code]) return;
    room.stage = 4;
    room.stage4Reported = new Set();
    io.to(code).emit('stage4_start', {});
  }, CONFIG.STAGE_TRANSITION_MS);
}

function finishGame(code) {
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
  io.to(code).emit('game_over', { scores: room.scores, names: room.names });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Chase game server running on port ${PORT}`));
