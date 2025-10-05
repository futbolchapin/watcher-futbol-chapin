// watcher-futbol-chapin – Worker FCM HTTP v1
// Eventos: Alineaciones, Inicio (0->1), Entretiempo (5), Segundo Tiempo (6), Final (2), Gol
// Envia a: match_{matchId}, team_{homeTeamId}, team_{awayTeamId} y LEGACY_TOPIC (opcional)

console.log('[watcher] iniciado');

// ===== 1) Config =====
const POLL_MS = Number(process.env.POLL_MS || 15000); // 15s por defecto
const FEED_TMPL = process.env.FEED_TMPL || ''; // p.ej: https://api.tu-dominio.com/match/{id}.json
const FEED_LIST_URL = process.env.FEED_LIST_URL || ''; // opcional: URL que devuelve lista de matchIds
const MATCH_IDS = (process.env.MATCH_IDS || '')
  .split(',').map(s => s.trim()).filter(Boolean); // alternativa a FEED_LIST_URL

const SCOPE_DEFAULT = process.env.SCOPE_DEFAULT || 'guatemala';
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'match_';
const TEAM_PREFIX  = process.env.TEAM_PREFIX  || 'team_';
const LEGACY_TOPIC = process.env.LEGACY_TOPIC || ''; // opcional (compatibilidad clientes viejos)
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

// Detecta claves comunes del JSON (DataFactory u otros) sin romper si cambian los nombres
const pick = (obj, keys, dflt = undefined) => {
  for (const k of keys) {
    const v = k.split('.').reduce((o, p) => (o ? o[p] : undefined), obj);
    if (v !== undefined && v !== null) return v;
  }
  return dflt;
};

// --- helpers para normalizar ---
const toNum = (v, d = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const toBool = (v) => {
  if (v === true || v === 'true' || v === 1 || v === '1') return true;
  if (Array.isArray(v)) return v.length > 0;
  return !!v;
};

// Normaliza el JSON del partido a un formato estándar (DataFactory-friendly)
const normalizeMatch = (raw) => {
  const statusId = toNum(pick(raw, [
    'statusId',
    'status.statusId',        // DF: { status: { statusId: ... } }
    'match.status.statusId',
    'match.statusId',
    'live.statusId'
  ], 0));

  const homeGoals = toNum(pick(raw, [
    'homeGoals',
    'homeScore',
    'score.home',
    'home.goals',
    'match.homeGoals'
  ], 0));

  const awayGoals = toNum(pick(raw, [
    'awayGoals',
    'awayScore',
    'score.away',
    'away.goals',
    'match.awayGoals'
  ], 0));

  const lineupsPublished = toBool(pick(raw, [
    'lineupsPublished',
    'hasLineups',
    'lineupConfirmed',
    'lineUpConfirmed',
    'lineups.home.length'
  ], false));

  const homeTeamId = String(pick(raw, [
    'homeTeamId',
    'home.id',
    'homeTeam.id',
    'teams.home.id',
    'homeTeam.teamId'
  ], ''));

  const awayTeamId = String(pick(raw, [
    'awayTeamId',
    'away.id',
    'awayTeam.id',
    'teams.away.id',
    'awayTeam.teamId'
  ], ''));

  const homeName = String(pick(raw, [
    'homeName',
    'home.name',
    'homeTeam',           // DF suele traer "homeTeam": "Aurora"
    'homeTeam.name',
    'teams.home.name'
  ], 'Local'));

  const awayName = String(pick(raw, [
    'awayName',
    'away.name',
    'awayTeam',
    'awayTeam.name',
    'teams.away.name'
  ], 'Visitante'));

  const scope   = String(pick(raw, ['scope', 'tournament.scope', 'channel'], SCOPE_DEFAULT));
  const matchId = String(pick(raw, ['matchId', 'id', 'eventId'], ''));
  const minute  = String(pick(raw, ['minute', 'clock.minute', 'live.minute', 'match.minute'], ''));

  return {
    matchId, scope,
    statusId, homeGoals, awayGoals, lineupsPublished,
    homeTeamId, awayTeamId, homeName, awayName, minute
  };
};


// Construye deeplink/tab por evento
const tabForEvent = (evt) => {
  switch (evt) {
    case 'LINEUPS': return 'alineaciones';
    case 'START':   return 'en_vivo';
    case 'HT':      return 'en_vivo';
    case 'ST':      return 'en_vivo';
    case 'END':     return 'resumen';
    case 'GOAL':    return 'en_vivo';
    default:        return 'resumen';
  }
};

// Mensajes bonitos
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
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
};

const getMatchUrl = (id) => {
  if (!FEED_TMPL.includes('{id}')) throw new Error('FEED_TMPL debe contener {id}');
  return FEED_TMPL.replace('{id}', String(id));
};

const loadMatchIds = async () => {
  if (FEED_LIST_URL) {
    try {
      const data = await fetchJson(FEED_LIST_URL);
      // Adapta según tu feed: aceptar [123,456] o [{matchId:123},...]
      const ids = Array.isArray(data)
        ? data.map(x => (typeof x === 'object' ? x.matchId ?? x.id : x))
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
    screen: 'match',
    tab: tabForEvent(evt),
    matchId: String(m.matchId),
    scope: m.scope || SCOPE_DEFAULT,
    event: evt,
    statusId: String(m.statusId),
    homeGoals: String(m.homeGoals),
    awayGoals: String(m.awayGoals)
  };
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
  console.log('[debug]', m.matchId || id, 'status=', m.statusId, 'score=', m.homeGoals + '-' + m.awayGoals, 'lineups=', m.lineupsPublished, 'names=', m.homeName, 'vs', m.awayName);

  if (!m.matchId) m.matchId = String(matchId); // respaldo

  const prev = last.get(m.matchId);
  if (!prev) {
    last.set(m.matchId, { ...m });
    console.log('[state] init', m.matchId, m.homeName, 'vs', m.awayName);
    // Si quieres notificar al arrancar, podrías enviar un "init" aquí.
    return;
  }

  // --- Eventos ---
  // Alineaciones
  if (!prev.lineupsPublished && m.lineupsPublished) {
    await handleEvent('LINEUPS', m);
  }
  // Cambios de estado relevantes
  if (prev.statusId !== m.statusId) {
    if (prev.statusId === 0 && m.statusId === 1) await handleEvent('START', m);
    if (m.statusId === 5) await handleEvent('HT', m);   // Entretiempo
    if (m.statusId === 6) await handleEvent('ST', m);   // Segundo tiempo
    if (m.statusId === 2) await handleEvent('END', m);  // Final
  }
  // Goles
  if (m.homeGoals > prev.homeGoals || m.awayGoals > prev.awayGoals) {
    await handleEvent('GOAL', m);
  }

  last.set(m.matchId, { ...m });
};

const loop = async () => {
  // Envío de prueba al arrancar (opcional)
  if (SEND_TEST_ON_BOOT) {
    await sendToTopics(
      [LEGACY_TOPIC].filter(Boolean),
      { title: 'Watcher OK', body: 'Arrancó correctamente.' },
      { screen: 'home', tab: 'home' }
    );
  }

  let ids = await loadMatchIds();
  if (!ids.length) {
    console.warn('⚠️ No hay MATCH_IDS ni FEED_LIST_URL con ids válidos.');
  }

  while (true) {
    try {
      // refresca lista de ids si usas FEED_LIST_URL
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

