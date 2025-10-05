console.log('[watcher] iniciado');

const tick = () => {
  const now = new Date().toISOString();
  console.log(`[watcher] vivo ${now}`);
};
const timer = setInterval(tick, 60_000);

process.on('SIGTERM', () => {
  console.log('[watcher] apagando…');
  clearInterval(timer);
  process.exit(0);
});
