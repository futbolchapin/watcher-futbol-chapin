// watcher-futbol-chapin – Worker FCM HTTP v1
// Eventos: Alineaciones, Inicio (0->1), Entretiempo (5), Segundo Tiempo (6), Final (2), Gol
// Envía a: match_{matchId}, team_{homeTeamId}, team_{awayTeamId} y LEGACY_TOPIC (opcional)

console.log('[watcher] iniciado');

// ===== 1) Config =====
const POLL_MS = Number(process.env.POLL_MS || 15000); // 15s por defecto
const FEED_TMPL = process.env.FEED_TMPL || ''; // ej: https://.../events/{id}.json
const FEED_LIST_URL = process.env.FEED_LIST_URL || ''; // opcional: URL que devuelve lista de matchIds
const MATCH_IDS = (process.env.MATCH_IDS || '')
  .split(',').map(s => s.trim()).filter(Boolean); // alternativa a FEED_LIST_URL

const SCOPE_DEFAULT = process.env.SCOPE_DEFAULT || 'guatemala';
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'match_';
const TEAM_PREFIX  = process.env.TEAM_PREFIX  || 'team_';
const LEGACY_TOPIC = process.env.LEGACY_TOPIC || ''; // opcional (compat clientes viejos)
const SEND_TEST_ON_BOOT = process.env.SEND_TEST_ON_BOOT === '1'; // opcional

// ===== 2) Firebase Admin =====
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

// ===== 3) Utilidades =====
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

const sendToTopics = async (topics, notification, data = {}) => {
  const messages = topics.filter(Boolean).map(topic => ({
    topic,
    notification, // { title, body }
    data: Object.fromEntries(Object.entries(data).map(([k, v]) => [k, String(v)])),
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

// Acceso tolerante
const pick = (obj, keys, dflt = undefined) => {
  for (const k of keys) {
    const v = k.split('.').reduce((o, p) => (o ? o[p] : undefined), obj);
    if (v !== undefined && v !== null) return v;
  }
  return dflt;
};

// helpers
const toNum = (v, d = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const toBool = (v) => {
  if (v === true || v === 'true' || v === 1 || v === '1') return true;
  if (Array.isArray(v)) return v.length > 0;
  return !!v;
};

// REEMPLAZA normalizeMatch COMPLETA POR ESTA VERSIÓN
const normalizeMatch = (raw) => {
  const m  = raw.match || {};
  const st = raw.status || {};
  const teamsMap = raw.teams || {};

  // IDs y nombres (vienen en match.*)
  const homeTeamId = String(m.homeTeamId ?? pick(raw, ['homeTeamId','home.id','teams.home.id'], ''));
  const awayTeamId = String(m.awayTeamId ?? pick(raw, ['awayTeamId','away.id','teams.away.id'], ''));
  let homeName = String(m.homeTeamName ?? '');
  let awayName = String(m.awayTeamName ?? '');
  if (!homeName && homeTeamId && teamsMap[homeTeamId]?.name) homeName = teamsMap[homeTeamId].name;
  if (!awayName && awayTeamId && teamsMap[awayTeamId]?.name) awayName = teamsMap[awayTeamId].name;
  if (!homeName) homeName = 'Local';
  if (!awayName) awayName = 'Visitante';

  // Estado / minuto
  const statusId = toNum(st.statusId ?? raw.statusId, 0);
  const minute   = String(pick(raw, ['minute','live.minute','match.minute'], ''));

  // Marcador: objeto scoreStatus/scoresStatus/scores con llaves = teamId
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

  // Fallback por si no viene el objeto anterior
  if (homeGoals == null) homeGoals = toNum(pick(raw, ['summary.goals.homeQty','homeGoals','homeScore','score.home'], 0));
  if (awayGoals == null) awayGoals = toNum(pick(raw, ['summary.goals.awayQty','awayGoals','awayScore','score.away'], 0));

  // Extras para deep-link
  const matchId = String(m.matchId ?? raw.matchId ?? raw.id ?? raw.eventId ?? '');
  const scope   = String(m.channel ?? raw.channel ?? raw.scope ?? SCOPE_DEFAULT);
  const ymd     = String(m.date ?? raw.date ?? '');
  const hhmm    = String(m.scheduledStart ?? raw.scheduledStart ?? '');
  const gmt     = String(m.gmt ?? raw.gmt ?? raw.stadiumGMT ?? '');

  // Alineaciones
  const lineupsPublished = toBool(st.lineUpConfirmed ?? raw.lineUpConfirmed ?? pick(raw, ['lineups.home.length'], false));

  return {
    matchId, scope,
    statusId, homeGoals, awayGoals, lineupsPublished,
    homeTeamId, awayTeamId, homeName, awayName, minute,
    ymd, hhmm, gmt
  };
};



// Pestaña correcta para tu app (con espacio)
const tabForEvent = (evt) => {
  switch (evt) {
    case 'LINEUPS': return 'alineaciones';
    case 'START':   return 'en vivo';
    case 'HT':      return 'detalles';
    case 'ST':      return 'en vivo';
    case 'END':     return 'detalles';
    case 'GOAL':    return 'en vivo';
    default:        return 'detalles';
  }
};

// Títulos y cuerpos
const titleFor = (evt, m) => {
  switch (evt) {
    case 'LINEUPS': return `Alineaciones: ${m.homeName} vs ${m.awayName}`;
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
    case 'START':   return 'Sigue el minuto a minuto en la app.';
    case 'HT':      return 'Entretiempo (statusId 5).';
    case 'ST':      return 'Comienza el segundo tiempo.';
    case 'END':     return 'Mira el resumen y estadísticas.';
    case 'GOAL':    return `Min ${m.minute || ''} | ${m.homeName} ${m.homeGoals}-${m.awayGoals} ${m.awayName}`;
    default:        return '';
  }
};

// ===== 4) Core del watcher =====
const last = new Map(); // estado previo por matchId

const fetchJson = async (url) => {
  const res = await fetch(url, { headers: { 'cache-control': 'no-cache', 'pragma': 'no-cache' } });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
};

const getMatchUrl = (id) => {
  if (!FEED_TMPL.includes('{id}')) throw new Error('FEED_TMPL debe contener {id}');
  const base = FEED_TMPL.replace('{id}', String(id));
  const sep = base.includes('?') ? '&' : '?';
  // param anti-caché (cada poll trae la versión más reciente)
  return `${base}${sep}cb=${Date.now()}`;
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

  // Deep-link alineado a tu app
  const data = {
    screen: 'Match',
    tab: tabForEvent(evt),
    matchId: String(m.matchId),
    channel: m.scope || SCOPE_DEFAULT,
    event: evt,
    statusId: String(m.statusId),
    homeGoals: String(m.homeGoals),
    awayGoals: String(m.awayGoals)
  };
  if (m.ymd)  data.ymd  = m.ymd;
  if (m.hhmm) data.hhmm = m.hhmm;
  if (m.gmt)  data.gmt  = m.gmt;

  await sendToTopics(topics, notification, data);
};

const tickMatch = async (matchId) => {
  const url = getMatchUrl(matchId);
  let raw;
  try {
    raw = await fetchJson(url);
  } catch (e) {
    console.error('[fetch]', matchId, e.message);
    return;
  }
  const m = normalizeMatch(raw);
  if (!m.matchId) m.matchId = String(matchId); // respaldo

  // debug visible
  console.log('[debug]', m.matchId, 'status=', m.statusId, 'score=', `${m.homeGoals}-${m.awayGoals}`, 'lineups=', m.lineupsPublished, '|', m.homeName, 'vs', m.awayName);

  const prev = last.get(m.matchId);
  if (!prev) {
    last.set(m.matchId, { ...m });
    console.log('[state] init', m.matchId, m.homeName, 'vs', m.awayName);
    return;
  }

  // --- Eventos ---
  if (!prev.lineupsPublished && m.lineupsPublished) {
    await handleEvent('LINEUPS', m);
  }

  if (prev.statusId !== m.statusId) {
    if (prev.statusId === 0 && m.statusId === 1) await handleEvent('START', m);
    if (m.statusId === 5) await handleEvent('HT', m);   // Entretiempo
    if (m.statusId === 6) await handleEvent('ST', m);   // Segundo tiempo
    if (m.statusId === 2) await handleEvent('END', m);  // Final
  }

  if (m.homeGoals > prev.homeGoals || m.awayGoals > prev.awayGoals) {
    await handleEvent('GOAL', m);
  }

  last.set(m.matchId, { ...m });
};

const loop = async () => {
  if (SEND_TEST_ON_BOOT) {
    await sendToTopics(
      [LEGACY_TOPIC].filter(Boolean),
      { title: 'Watcher OK', body: 'Arrancó correctamente.' },
      { screen: 'Match', tab: 'detalles' }
    );
  }

  let ids = await loadMatchIds();
  if (!ids.length) {
    console.warn('⚠️ No hay MATCH_IDS ni FEED_LIST_URL con ids válidos.');
  }

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
