// watcher-futbol-chapin – FCM HTTP v1 + PRE30 + Redis idempotente + lista desde home-week.json

const WATCHER_ID = process.env.WATCHER_ID || Math.random().toString(36).slice(2, 8);
console.log(`[watcher ${WATCHER_ID}] iniciado`);

// ===== 1) Config =====
const POLL_MS = Number(process.env.POLL_MS || 15000);
const FEED_TMPL = process.env.FEED_TMPL || '';
const FEED_LIST_URL = process.env.FEED_LIST_URL || ''; // ej: https://futbolchapin.net/edit/home-week.json
const MATCH_IDS = (process.env.MATCH_IDS || '').split(',').map(s => s.trim()).filter(Boolean);

const SCOPE_DEFAULT = process.env.SCOPE_DEFAULT || 'guatemala';
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'match_';
const TEAM_PREFIX  = process.env.TEAM_PREFIX  || 'team_';
const LEGACY_TOPIC = process.env.LEGACY_TOPIC || '';
const SEND_TEST_ON_BOOT = process.env.SEND_TEST_ON_BOOT === '1';

const ENABLE_MATCH_TOPICS = process.env.ENABLE_MATCH_TOPICS !== '0';
const ENABLE_TEAM_TOPICS  = process.env.ENABLE_TEAM_TOPICS  !== '0';
const ENABLE_NEWS_TOPIC   = process.env.ENABLE_NEWS_TOPIC === '1';
const NEWS_TOPIC          = process.env.NEWS_TOPIC || 'news_guatemala';
const NEWS_EVENTS         = (process.env.NEWS_EVENTS || 'START,END')
  .split(',').map(s => s.trim().toUpperCase()).filter(Boolean);

const NEWS_ONLY_TEAM_IDS  = (process.env.NEWS_ONLY_TEAM_IDS || '')
  .split(',').map(s => s.trim()).filter(Boolean);

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
      ...(isTls ? { tls: { rejectUnauthorized: false } } : {}),
      maxRetriesPerRequest: 1,
      enableReadyCheck: false,
    });

    redis.on('connect', () => console.log('[redis] connect ok'));
    redis.on('error', (e) => console.error('[redis] error', e.message));
  }
} catch (e) {
  console.warn('[redis] no disponible', e.message);
}

const sentKey = (m, tag) => `sent:${m.matchId}:${tag}`;

// Bloqueo atómico: sólo un proceso gana (Redis SET NX)
const tryMarkOnce = async (key, ttlSec = 36 * 3600) => {
  if (!redis) {
    if (mem.has(key)) return false;
    mem.add(key);
    setTimeout(() => mem.delete(key), ttlSec * 1000);
    return true;
  }
  try {
    const ok = await redis.set(key, '1', 'NX', 'EX', ttlSec);
    return ok === 'OK';
  } catch {
    if (mem.has(key)) return false;
    mem.add(key);
    setTimeout(() => mem.delete(key), ttlSec * 1000);
    return true;
  }
};

// ===== 4) Utils =====
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

const sendCondition = async (condition, notification, data = {}) => {
  const message = {
    condition,
    notification,
    data: Object.fromEntries(Object.entries(data).map(([k, v]) => [k, String(v)])),
    android: { priority: 'high' },
    apns: { headers: { 'apns-priority': '10' } },
  };
  try {
    const id = await admin.messaging().send(message);
    console.log('[push]', WATCHER_ID, 'ok', condition, id, notification.title);
  } catch (e) {
    console.error('[push]', WATCHER_ID, 'error', condition, e.message);
  }
};

const pick = (obj, keys, dflt = undefined) => {
  for (const k of keys) {
    const v = k.split('.').reduce((o, p) => (o ? o[p] : undefined), obj);
    if (v !== undefined && v !== null) return v;
  }
  return dflt;
};
const toNum = (v, d = 0) => (Number.isFinite(Number(v)) ? Number(v) : d);
const toBool = (v) => (v === true || v === 'true' || v === 1 || v === '1' || (Array.isArray(v) && v.length > 0));
const parseOffsetMin = (val) => {
  if (!val) return 0;
  const s = String(val).trim();
  if (s.includes(':')) {
    const m = s.match(/^([+-])?(\d{1,2}):?(\d{2})$/);
    if (m) {
      const sign = m[1] === '-' ? -1 : 1;
      return sign * (Number(m[2]) * 60 + Number(m[3] || '0'));
    }
  }
  const n = Number(s);
  return Number.isFinite(n) ? n * 60 : 0;
};

// ===== 5) Normalización de datos =====
const normalizeMatch = (raw) => {
  const m = raw.match || {};
  const st = raw.status || {};
  const teamsMap = raw.teams || {};

  const homeTeamId = String(m.homeTeamId ?? pick(raw, ['homeTeamId', 'home.id', 'teams.home.id'], ''));
  const awayTeamId = String(m.awayTeamId ?? pick(raw, ['awayTeamId', 'away.id', 'teams.away.id'], ''));
  let homeName = m.homeTeamName || teamsMap[homeTeamId]?.name || 'Local';
  let awayName = m.awayTeamName || teamsMap[awayTeamId]?.name || 'Visitante';

  const statusId = toNum(st.statusId ?? raw.statusId, 0);
  const scoresObj = raw.scoreStatus || raw.scoresStatus || raw.scores || m.scores || null;
  const readScore = (obj, id) => (obj?.[id]?.score ?? obj?.[id]?.goals ?? null);

  let homeGoals = toNum(readScore(scoresObj, homeTeamId) ?? pick(raw, ['summary.goals.homeQty'], 0));
  let awayGoals = toNum(readScore(scoresObj, awayTeamId) ?? pick(raw, ['summary.goals.awayQty'], 0));

  const matchId = String(m.matchId ?? raw.matchId ?? raw.id ?? raw.eventId ?? '');
  const scope = String(m.scope ?? raw.scope ?? SCOPE_DEFAULT).toLowerCase();
  const ymd = String(m.date ?? raw.date ?? '');
  const hhmm = String((m.scheduledStart ?? raw.scheduledStart ?? '').toString().slice(0, 5));
  const gmt = String(m.gmt ?? raw.gmt ?? '');
  const lineupsPublished = toBool(st.lineUpConfirmed ?? raw.lineUpConfirmed ?? pick(raw, ['lineups.home.length'], false));

  return { matchId, scope, statusId, homeGoals, awayGoals, lineupsPublished, homeTeamId, awayTeamId, homeName, awayName, ymd, hhmm, gmt };
};

const parseStartUtc = (m) => {
  if (!m.ymd || !m.hhmm) return null;
  const [H, Mi] = m.hhmm.split(':').map(Number);
  const offsetMin = parseOffsetMin(m.gmt);
  return Date.UTC(+m.ymd.slice(0,4), +m.ymd.slice(4,6)-1, +m.ymd.slice(6,8), H, Mi) - offsetMin * 60000;
};

const tabForEvent = (evt) => {
  switch (evt) {
    case 'LINEUPS': return 'alineaciones';
    case 'START': case 'ST': case 'GOAL': return 'en vivo';
    case 'HT': case 'END': case 'PRE30': default: return 'detalles';
  }
};
const teamPair = (m) => `${m.homeName} vs ${m.awayName}`;
const pairWithScore = (m) => `${m.homeName} ${m.homeGoals} - ${m.awayGoals} ${m.awayName}`;
const titleFor = (evt, m, extra = {}) => ({
  LINEUPS: 'Lineups: Alineaciones confirmadas',
  PRE30: teamPair(m),
  START: 'Inicia el partido',
  HT: 'Termina el primer tiempo',
  ST: 'Inicia el segundo tiempo',
  END: 'Termina el partido',
  GOAL: extra.scorer ? `¡Goooool! de ${extra.scorer === 'home' ? m.homeName : m.awayName}` : '¡Goooool!',
}[evt] || teamPair(m));
const bodyFor = (evt, m) => ({
  LINEUPS: `${teamPair(m)}. Toca para ver el 11 inicial`,
  PRE30: 'El partido comenzará en 30 minutos',
  START: `${teamPair(m)}. Toca para ver el minuto a minuto`,
  HT: pairWithScore(m),
  ST: pairWithScore(m),
  END: pairWithScore(m),
  GOAL: pairWithScore(m),
}[evt] || '');

const fetchJson = async (url) => {
  const res = await fetch(url, { headers: { 'cache-control': 'no-cache' } });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
};

// ===== 6) Lógica principal =====
const getMatchUrl = (id, scope = SCOPE_DEFAULT) => {
  let base = FEED_TMPL.replace('{id}', String(id)).replace('{scope}', scope);
  const sep = base.includes('?') ? '&' : '?';
  return `${base}${sep}cb=${Date.now()}`;
};

const loadMatchList = async () => {
  try {
    const data = await fetchJson(`${FEED_LIST_URL}?cb=${Date.now()}`);
    if (Array.isArray(data)) {
      return data.map(x => ({
        id: String(x.matchId ?? x.id ?? x.eventId ?? x),
        scope: String(x.scope ?? SCOPE_DEFAULT).toLowerCase(),
      })).filter(it => it.id);
    }
    if (data?.events) {
      const out = Object.keys(data.events).map(k => {
        const parts = k.split('.');
        const id = parts.pop();
        const idx = parts.indexOf('futbol');
        const scope = idx >= 0 && parts[idx + 1] ? parts[idx + 1] : SCOPE_DEFAULT;
        return { id, scope: scope.toLowerCase() };
      });
      const seen = new Set();
      return out.filter(it => (seen.has(it.id) ? false : seen.add(it.id)));
    }
  } catch (e) {
    console.error('[ids]', e.message);
  }
  return [];
};

const handleEvent = async (evt, m, extra = {}) => {
  const notification = { title: titleFor(evt, m, extra), body: bodyFor(evt, m) };
  const data = { screen: 'Match', tab: tabForEvent(evt), matchId: m.matchId, channel: m.scope, event: evt };

  const parts = [];
  if (ENABLE_MATCH_TOPICS) parts.push(`'${TOPIC_PREFIX}${m.matchId}' in topics`);
  if (ENABLE_TEAM_TOPICS && m.homeTeamId) parts.push(`'${TEAM_PREFIX}${m.homeTeamId}' in topics`);
  if (ENABLE_TEAM_TOPICS && m.awayTeamId) parts.push(`'${TEAM_PREFIX}${m.awayTeamId}' in topics`);
  if (ENABLE_NEWS_TOPIC && NEWS_EVENTS.includes(evt)) parts.push(`'${NEWS_TOPIC}' in topics`);

  if (parts.length) await sendCondition(parts.join(' || '), notification, data);
};

const last = new Map();

const tickMatch = async ({ id: matchId, scope: scopeHint }) => {
  try {
    const m = normalizeMatch(await fetchJson(getMatchUrl(matchId, scopeHint)));
    if (!m.matchId) m.matchId = String(matchId);
    if (!m.scope) m.scope = scopeHint || SCOPE_DEFAULT;
    console.log('[debug]', WATCHER_ID, m.matchId, 'status', m.statusId, 'score', `${m.homeGoals}-${m.awayGoals}`);

    const startUtc = parseStartUtc(m);
    if (startUtc) {
      const diffMs = startUtc - Date.now();
      if (diffMs > 0 && diffMs <= PRE_PUSH_MIN * 60000) {
        const key = sentKey(m, `pre${PRE_PUSH_MIN}`);
        if (await tryMarkOnce(key)) await handleEvent('PRE30', m);
      }
    }

    const prev = last.get(m.matchId);
    if (!prev) return void last.set(m.matchId, { ...m });

    if (!prev.lineupsPublished && m.lineupsPublished && await tryMarkOnce(sentKey(m, 'lineups'))) {
      await handleEvent('LINEUPS', m);
    }

    if (prev.statusId !== m.statusId) {
      if (prev.statusId === 0 && m.statusId === 1 && await tryMarkOnce(sentKey(m, 'start')))
        await handleEvent('START', m);
      if (m.statusId === 5 && await tryMarkOnce(sentKey(m, 'ht')))
        await handleEvent('HT', m);
      if (m.statusId === 6 && await tryMarkOnce(sentKey(m, 'st')))
        await handleEvent('ST', m);
      if (m.statusId === 2 && await tryMarkOnce(sentKey(m, 'end')))
        await handleEvent('END', m);
    }

    if (m.homeGoals > prev.homeGoals || m.awayGoals > prev.awayGoals) {
      const key = sentKey(m, `goal:${m.homeGoals}-${m.awayGoals}`);
      if (await tryMarkOnce(key, 24 * 3600)) {
        const extra = { scorer: m.homeGoals > prev.homeGoals ? 'home' : 'away' };
        await handleEvent('GOAL', m, extra);
      }
    }

    last.set(m.matchId, { ...m });
  } catch (e) {
    console.error('[tick]', WATCHER_ID, matchId, e.message);
  }
};

const loop = async () => {
  if (SEND_TEST_ON_BOOT)
    await sendCondition(`'${LEGACY_TOPIC}' in topics`, { title: 'Watcher OK', body: 'Arrancó correctamente.' });

  let list = await loadMatchList();
  if (!list.length) console.warn('⚠️ No hay partidos activos.');
  while (true) {
    try {
      if (FEED_LIST_URL) list = await loadMatchList();
      await Promise.all(list.map(item => tickMatch(item)));
    } catch (e) {
      console.error('[loop]', WATCHER_ID, e.message);
    }
    await sleep(POLL_MS);
  }
};

loop().catch(e => console.error('[fatal]', WATCHER_ID, e));
process.on('SIGTERM', () => { console.log('[watcher]', WATCHER_ID, 'apagando…'); process.exit(0); });
