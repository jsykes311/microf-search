/* CARTO browser keys are domain-restricted in the CARTO dashboard. */
async function addMicrofBasemap(map) {
  try {
    const response = await fetch('/api/map-config', { cache: 'no-store' });
    if (!response.ok) throw new Error('Map configuration unavailable');
    const { cartoKey } = await response.json();
    if (!cartoKey) throw new Error('Map background is not configured');
    L.tileLayer(
      'https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png?key=' + encodeURIComponent(cartoKey),
      {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="https://carto.com/attributions">CARTO</a>',
        maxZoom: 20,
        zIndex: 0
      }
    ).addTo(map);
  } catch (error) {
    console.error('Map background could not load:', error.message);
    const notice = L.control({ position: 'bottomleft' });
    notice.onAdd = function () {
      const el = L.DomUtil.create('div');
      el.textContent = 'Map background unavailable. Dealer search is still available.';
      el.style.cssText = 'background:white;padding:8px 12px;max-width:240px;color:#374151;border-radius:6px;box-shadow:0 1px 5px #0003';
      return el;
    };
    notice.addTo(map);
  }
}
