/**
 * Microf-Search status widget
 * Include on any page — injects a sticky badge and polls /api/health every 30s.
 * Also drives .nav-status-dot / .nav-status-label on pages that already have them.
 */
(function () {
  const POLL_MS   = 30_000;
  const BADGE_ID  = 'ms-status-badge';

  const STATES = {
    online:  { label: 'Microf-Search Online',      color: '#34c759', glow: 'rgba(52,199,89,0.25)'  },
    warming: { label: 'Microf-Search Warming Up',  color: '#ff9f0a', glow: 'rgba(255,159,10,0.25)' },
    offline: { label: 'Microf-Search Offline',     color: '#ff3b30', glow: 'rgba(255,59,48,0.25)'  },
  };

  /* ── Create the floating badge ── */
  function createBadge() {
    if (document.getElementById(BADGE_ID)) return;
    const el = document.createElement('div');
    el.id = BADGE_ID;
    el.style.cssText = [
      'position:fixed', 'bottom:14px', 'right:16px', 'z-index:9999',
      'display:flex', 'align-items:center', 'gap:6px',
      'background:rgba(255,255,255,0.92)', 'backdrop-filter:blur(6px)',
      'border:1px solid rgba(0,0,0,0.09)', 'border-radius:20px',
      'padding:5px 10px 5px 8px', 'box-shadow:0 2px 8px rgba(0,0,0,0.12)',
      'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif',
      'font-size:0.7rem', 'font-weight:500', 'color:#444',
      'pointer-events:none', 'transition:opacity 0.3s',
    ].join(';');
    el.innerHTML = `
      <span id="ms-badge-dot" style="width:7px;height:7px;border-radius:50%;flex-shrink:0;transition:background 0.4s,box-shadow 0.4s;"></span>
      <span id="ms-badge-label">Checking…</span>`;
    document.body.appendChild(el);
  }

  /* ── Apply a state to all status elements on the page ── */
  function applyState(key) {
    const s = STATES[key] || STATES.offline;
    const dotStyle = `background:${s.color};box-shadow:0 0 0 2.5px ${s.glow}`;

    // Floating badge
    const bd = document.getElementById('ms-badge-dot');
    const bl = document.getElementById('ms-badge-label');
    if (bd) { bd.style.cssText = dotStyle; }
    if (bl) { bl.textContent = s.label; }

    // index.html sidebar dot/label (if present)
    document.querySelectorAll('.nav-status-dot').forEach(d => { d.style.cssText = dotStyle + ';width:8px;height:8px;border-radius:50%;flex-shrink:0'; });
    document.querySelectorAll('.nav-status-label').forEach(l => { l.textContent = s.label; });
  }

  /* ── Poll /api/health ── */
  async function poll() {
    try {
      const ctrl = new AbortController();
      const tid  = setTimeout(() => ctrl.abort(), 5000);
      const res  = await fetch('/api/health', { signal: ctrl.signal });
      clearTimeout(tid);
      if (!res.ok) { applyState('offline'); return; }
      const data = await res.json();
      applyState(data.status || 'offline');
    } catch (_) {
      applyState('offline');
    }
  }

  /* ── Boot ── */
  function init() {
    createBadge();
    applyState('warming');   // optimistic default while first poll runs
    poll();
    setInterval(poll, POLL_MS);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
