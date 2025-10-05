// watcher-futbol-chapin – FCM HTTP v1 + PRE30 + Redis idempotente

console.log('[watcher] iniciado');

// ===== 1) Config =====
const POLL_MS = Number(process.env.POLL_MS || 15000);
const FEED_TMPL = process.env.FEED_TMPL || '';
const FEED_LIST_URL = process.env.FEED_LIST_URL || '';
const MATCH_IDS = (process.env.MATCH_IDS || '').split(',').map(s => s.trim()).filter(Boolean);

const SCOPE_DEFAULT = process.env.SCOPE_DEFAULT || 'guatemala';
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'match_';
const TEAM_PREFIX  = process.env.TEAM_PREFIX  || 'team_';
const LEGACY_TOPIC = process.env.LEGACY_TOPIC || '';
const SEND_TEST_ON_BOOT = process.env.SEND_TEST_ON_BOOT === '1';

// ➕ minutos antes del inicio para PRE30 (puedes cambiar a 1 para probar rápido)
const PRE_PUSH_MIN = Number(process.env.PRE_PUSH_MIN || 30);

// ===== 2) Firebase =====
const admin = require('firebase-admin');
if (!process.env.FCM_PROJECT_ID || !process.env.FCM_CLIENT_EMAIL || !process.env.FCM_PRIVATE_KEY) {
  console.error('⚠️ Faltan credenciales FCM en Config Vars');
}
const serviceAccount = {
  projectId: process.env.FCM_PROJECT_ID,
  clientEmail: process.env.FCM_CLIENT_EMAIL,
  privateKey: process.env.FCM_PRIVATE_KEY.replace(/\\n/g, '\n'),
};
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });

// ===== 3) Redis (idempotencia) =====
let redis = null;
const mem = new Set();
try {
  if (process.env.REDIS_URL) {
    const IORedis = require('ioredis');
    const url = process.env.REDIS_URL;
    const isTls = url.startsWith('rediss://');

    redis = new IORedis(url, {
      // Aceptar cert self-signed del add-on cuando usa TLS
      ...(isTls ? { tls: { rejectUnauthorized: false } } : {}),
      // Evitar que ioredis reintente eternamente y corte el loop
      maxRetriesPerRequest: 1,
      enableReadyCheck: false,
    });

    redis.on('connect', () => console.log('[redis] connect ok'));
    redis.on('error',   (e) => console.error('[redis] error', e.message));
  }
} catch (e) {
  console.warn('[redis] no disponible', e.message);
}

// Helpers con fallback a memoria si Redis falla
const sentKey = (m, tag) => `sent:${m.matchId}:${tag}`;
const wasSent = async (key) => {
  if (!redis) return mem.has(key);
  try { return (await redis.get(key)) === '1'; }
  catch { return mem.has(key); }
};
const markSent = async (key, ttlSec = 36 * 3600) => {
  if (!redis) { mem.add(key); setTimeout(() => mem.delete(key), ttlSec * 1000); return; }
  try { await redis.set(key, '1', 'EX', ttlSec); }
  catch { mem.add(key); setTimeout(() => mem.delete(key), ttlSec * 1000); }
};


// ===== 4) Utils =====
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

const sendToTopics = async (topics, notification, data = {}) => {
  const messages = topics.filter(Boolean).map(topic => ({
    topic,
    notification,
    data: Object.fromEntries(Object.entries(data).map(([k,v]) => [k, String(v)])),
    android: { priority: 'high' },
    apns: { headers: { 'apns-priority': '10' } }
  }));
  for (const msg of messages) {
    try {
      const id = await admin.messaging().send(msg);
      console.log('[push] ok', msg.topic, id, notification.title);
    } catch (e) {
      console.error('[push] error', msg.topic, e.message);
    }
  }
};

const pick = (obj, keys, dflt = undefined) => {
  for (const k of keys) {
    const v = k.split('.').reduce((o, p) => (o ? o[p] : undefined), obj);
    if (v !== undefined && v !== null) return v;
  }
  return dflt;
};
const toNum = (v, d = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const toBool = (v) => {
  if (v === true || v === 'true' || v === 1 || v === '1') return true;
  if (Array.isArray(v)) return v.length > 0;
  return !!v;
};
const parseOffsetMin = (val) => {
  if (val === undefined || val === null) return 0;
  const s = String(val).trim();
  if (s.includes(':')) {
    const m = s.match(/^([+-])?(\d{1,2}):?(\d{2})$/);
    if (m) { const sign = m[1] === '-' ? -1 : 1; return sign * (Number(m[2]) * 60 + Number(m[3] || '0')); }
  }
  const n = Number(s);
  return Number.isFinite(n) ? n * 60 : 0;
};

// Normaliza tu JSON (usa match.* y scoreStatus)
const normalizeMatch = (raw) => {
  const m  = raw.match || {};
  const st = raw.status || {};
  const teamsMap = raw.teams || {};

  const homeTeamId = String(m.homeTeamId ?? pick(raw, ['homeTeamId','home.id','teams.home.id'], ''));
  const awayTeamId = String(m.awayTeamId ?? pick(raw, ['awayTeamId','away.id','teams.away.id'], ''));
  let homeName = String(m.homeTeamName ?? '');
  let awayName = String(m.awayTeamName ?? '');
  if (!homeName && homeTeamId && teamsMap[homeTeamId]?.name) homeName = teamsMap[homeTeamId].name;
  if (!awayName && awayTeamId && teamsMap[awayTeamId]?.name) awayName = teamsMap[awayTeamId].name;
  if (!homeName) homeName = 'Local';
  if (!awayName) awayName = 'Visitante';

  const statusId = toNum(st.statusId ?? raw.statusId, 0);
  const minute   = String(pick(raw, ['minute','live.minute','match.minute'], ''));

  const scoresObj =
    raw.scoreStatus || raw.scoresStatus || raw.scores ||
    m.scoreStatus   || m.scoresStatus   || m.scores    || null;

  const readTeamScore = (obj, tid) => {
    if (!obj || !tid) return null;
    const key = String(tid);
    const v = obj[key] ?? obj[Number(key)];
    if (!v) return null;
    return toNum(v.score ?? v.value ?? v.goals ?? v.goalsQty, null);
  };
  let homeGoals = readTeamScore(scoresObj, homeTeamId);
  let awayGoals = readTeamScore(scoresObj, awayTeamId);
  if (homeGoals == null) homeGoals = toNum(pick(raw, ['summary.goals.homeQty','homeGoals','homeScore','score.home'], 0));
  if (awayGoals == null) awayGoals = toNum(pick(raw, ['summary.goals.awayQty','awayGoals','awayScore','score.away'], 0));

  // fechas para pre-partido
  const matchId = String(m.matchId ?? raw.matchId ?? raw.id ?? raw.eventId ?? '');
  const scope   = String(m.channel ?? raw.channel ?? raw.scope ?? SCOPE_DEFAULT);
  const ymd     = String(m.date ?? raw.date ?? '');
  const hhmm    = String((m.scheduledStart ?? raw.scheduledStart ?? '').toString().slice(0,5)); // HH:MM
  // Preferimos stadiumGMT si existe (más confiable para sede)
  const gmt     = String(m.stadiumGMT ?? raw.stadiumGMT ?? m.gmt ?? raw.gmt ?? '');

  const lineupsPublished = toBool(st.lineUpConfirmed ?? raw.lineUpConfirmed ?? pick(raw, ['lineups.home.length'], false));

  return { matchId, scope, statusId, homeGoals, awayGoals, lineupsPublished,
           homeTeamId, awayTeamId, homeName, awayName, minute, ymd, hhmm, gmt };
};

const parseStartUtc = (m) => {
  if (!m.ymd || !m.hhmm) return null;
  const Y = Number(m.ymd.slice(0,4));
  const M = Number(m.ymd.slice(4,6)) - 1;
  const D = Number(m.ymd.slice(6,8));
  const [H, Mi] = m.hhmm.split(':').map(Number);
  const offsetMin = parseOffsetMin(m.gmt);
  const localUtc = Date.UTC(Y, M, D, H, Mi);
  return localUtc - offsetMin * 60000; // pasa de hora local (GMT offset) a UTC
};

// Deeplink tab
const tabForEvent = (evt) => {
  switch (evt) {
    case 'LINEUPS': return 'alineaciones';
    case 'START':   return 'en vivo';
    case 'HT':      return 'detalles';
    case 'ST':      return 'en vivo';
    case 'END':     return 'detalles';
    case 'GOAL':    return 'en vivo';
    case 'PRE30':   return 'detalles';
    default:        return 'detalles';
  }
};

// Títulos / cuerpos
const titleFor = (evt, m) => {
  switch (evt) {
    case 'LINEUPS': return `Alineaciones: ${m.homeName} vs ${m.awayName}`;
    case 'PRE30':   return `Arranca en ${PRE_PUSH_MIN} min: ${m.homeName} vs ${m.awayName}`;
    case 'START':   return `¡Arranca! ${m.homeName} vs ${m.awayName}`;
    case 'HT':      return `Entretiempo: ${m.homeGoals}-${m.awayGoals}`;
    case 'ST':      return `Segundo tiempo en juego`;
    case 'END':     return `Final: ${m.homeName} ${m.homeGoals}-${m.awayGoals} ${m.awayName}`;
    case 'GOAL':    return `¡GOL! ${m.homeGoals}-${m.awayGoals}`;
    default:        return `${m.homeName} vs ${m.awayName}`;
  }
};
const bodyFor = (evt, m) => {
  switch (evt) {
    case 'LINEUPS': return 'Formaciones confirmadas. Toca para ver el 11.';
    case 'PRE30':   return m.hhmm ? `Hoy ${m.hhmm} (${m.gmt || 'GMT'})` : 'Faltan 30 minutos.';
    case 'START':   return 'Sigue el minuto a minuto en la app.';
    case 'HT':      return 'Entretiempo (statusId 5).';
    case 'ST':      return 'Comienza el segundo tiempo.';
    case 'END':     return 'Mira el resumen y estadísticas.';
    case 'GOAL':    return `Min ${m.minute || ''} | ${m.homeName} ${m.homeGoals}-${m.awayGoals} ${m.awayName}`;
    default:        return '';
  }
};

// ===== 5) Core =====
const fetchJson = async (url) => {
  const res = await fetch(url, { headers: { 'cache-control': 'no-cache', 'pragma': 'no-cache' } });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
};
const getMatchUrl = (id) => {
  if (!FEED_TMPL.includes('{id}')) throw new Error('FEED_TMPL debe contener {id}');
  const base = FEED_TMPL.replace('{id}', String(id));
  const sep = base.includes('?') ? '&' : '?';
  return `${base}${sep}cb=${Date.now()}`; // anti-cache
};
const loadMatchIds = async () => {
  if (FEED_LIST_URL) {
    try {
      const data = await fetchJson(FEED_LIST_URL);
      const ids = Array.isArray(data)
        ? data.map(x => (typeof x === 'object' ? (x.matchId ?? x.id) : x))
        : [];
      return ids.filter(Boolean).map(String);
    } catch (e) {
      console.error('[ids] error FEED_LIST_URL', e.message);
    }
  }
  return MATCH_IDS;
};

const handleEvent = async (evt, m) => {
  const topics = [
    `${TOPIC_PREFIX}${m.matchId}`,
    m.homeTeamId && `${TEAM_PREFIX}${m.homeTeamId}`,
    m.awayTeamId && `${TEAM_PREFIX}${m.awayTeamId}`,
    LEGACY_TOPIC || null
  ];
  const notification = { title: titleFor(evt, m), body: bodyFor(evt, m) };
  const data = {
    screen: 'Match',
    tab: tabForEvent(evt),
    matchId: String(m.matchId),
    channel: m.scope || SCOPE_DEFAULT,
    event: evt,
    statusId: String(m.statusId),
    homeGoals: String(m.homeGoals),
    awayGoals: String(m.awayGoals),
  };
  if (m.ymd)  data.ymd  = m.ymd;
  if (m.hhmm) data.hhmm = m.hhmm;
  if (m.gmt)  data.gmt  = m.gmt;
  await sendToTopics(topics, notification, data);
};

const last = new Map();

const tickMatch = async (matchId) => {
  const url = getMatchUrl(matchId);
  let raw;
  try { raw = await fetchJson(url); }
  catch (e) { console.error('[fetch]', matchId, e.message); return; }

  const m = normalizeMatch(raw);
  if (!m.matchId) m.matchId = String(matchId);

  // debug
  console.log('[debug]', m.matchId, 'status=', m.statusId, 'score=', `${m.homeGoals}-${m.awayGoals}`,
              'lineups=', m.lineupsPublished, '|', m.homeName, 'vs', m.awayName,
              '| ids=', m.homeTeamId, m.awayTeamId);

  // PRE30
  const startUtc = parseStartUtc(m);
  if (startUtc) {
    const diffMs = startUtc - Date.now();
    if (diffMs > 0 && diffMs <= PRE_PUSH_MIN * 60000) {
      const key = sentKey(m, `pre${PRE_PUSH_MIN}`);
      if (!(await wasSent(key))) {
        await handleEvent('PRE30', m);
        await markSent(key);
      }
    }
  }

  const prev = last.get(m.matchId);
  if (!prev) { last.set(m.matchId, { ...m }); console.log('[state] init', m.matchId, m.homeName, 'vs', m.awayName); return; }

  // Alineaciones
  if (!prev.lineupsPublished && m.lineupsPublished) {
    const key = sentKey(m, 'lineups');
    if (!(await wasSent(key))) { await handleEvent('LINEUPS', m); await markSent(key); }
  }

  // Estados
  if (prev.statusId !== m.statusId) {
    if (prev.statusId === 0 && m.statusId === 1) {
      const key = sentKey(m, 'start'); if (!(await wasSent(key))) { await handleEvent('START', m); await markSent(key); }
    }
    if (m.statusId === 5) { const key = sentKey(m, 'ht'); if (!(await wasSent(key))) { await handleEvent('HT', m); await markSent(key); } }
    if (m.statusId === 6) { const key = sentKey(m, 'st'); if (!(await wasSent(key))) { await handleEvent('ST', m); await markSent(key); } }
    if (m.statusId === 2) { const key = sentKey(m, 'end'); if (!(await wasSent(key))) { await handleEvent('END', m); await markSent(key); } }
  }

  // Gol (único por marcador)
  if (m.homeGoals > prev.homeGoals || m.awayGoals > prev.awayGoals) {
    const key = sentKey(m, `score-${m.homeGoals}-${m.awayGoals}`);
    if (!(await wasSent(key))) { await handleEvent('GOAL', m); await markSent(key); }
  }

  last.set(m.matchId, { ...m });
};

const loop = async () => {
  if (SEND_TEST_ON_BOOT) {
    await sendToTopics([LEGACY_TOPIC].filter(Boolean),
      { title: 'Watcher OK', body: 'Arrancó correctamente.' },
      { screen: 'Match', tab: 'detalles' });
  }

  let ids = await loadMatchIds();
  if (!ids.length) console.warn('⚠️ No hay MATCH_IDS ni FEED_LIST_URL con ids válidos.');

  while (true) {
    try {
      if (FEED_LIST_URL) ids = await loadMatchIds();
      await Promise.all(ids.map(id => tickMatch(id)));
    } catch (e) {
      console.error('[loop] error', e.message);
    }
    await sleep(POLL_MS);
  }
};

loop().catch(e => console.error('[fatal]', e));
process.on('SIGTERM', () => { console.log('[watcher] apagando…'); process.exit(0); });
