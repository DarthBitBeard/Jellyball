// Streaming-engine dashboard cards: Live Sessions (from /api/sessions plus
// /api/engine/status for breaker state) with ring-buffer sparklines, and the
// Legacy Proxy Fallbacks table.
(function () {
    const POINTS = 60; // 3 s polling -> three minutes of history
    const history = {}; // channel id -> {bitrate: [], latency: []}

    function esc(value) {
        return String(value ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'})[c]);
    }

    // Fixed-size ring buffer: the oldest value drops when full. Gaps are null.
    function pushRing(arr, value, max) {
        arr.push(value === undefined ? null : value);
        while (arr.length > (max || POINTS)) arr.shift();
        return arr;
    }

    function sparkline(values) {
        const nums = values.filter(v => typeof v === 'number');
        if (nums.length < 2) return '<span style="color: var(--text-muted);">-</span>';
        const w = 90, h = 22;
        const lo = Math.min(...nums), hi = Math.max(...nums), span = hi - lo || 1;
        const step = w / Math.max(values.length - 1, 1);
        const pts = [];
        values.forEach((v, i) => {
            if (typeof v === 'number') pts.push((i * step).toFixed(1) + ',' + (h - 2 - ((v - lo) / span) * (h - 4)).toFixed(1));
        });
        return '<svg width="' + w + '" height="' + h + '" viewBox="0 0 ' + w + ' ' + h + '" aria-hidden="true">' +
            '<polyline fill="none" stroke="var(--accent)" stroke-width="1.5" points="' + pts.join(' ') + '"/></svg>';
    }

    function fmt(value, unit, digits) {
        return typeof value === 'number' ? value.toFixed(digits || 0) + ' ' + unit : '-';
    }

    function render(sessions, status) {
        const body = document.getElementById('engine-sessions-body');
        if (!body) return;
        const ids = Object.keys(sessions || {}).sort();
        Object.keys(history).forEach(id => { if (!(id in sessions)) delete history[id]; });
        if (!ids.length) {
            body.innerHTML = '<tr><td colspan="7" style="padding: 0.5rem; color: var(--text-muted);">No channel sessions running. Sessions start when someone tunes in.</td></tr>';
        } else {
            body.innerHTML = ids.map(id => {
                const s = sessions[id];
                const h = history[id] || (history[id] = {bitrate: [], latency: []});
                pushRing(h.bitrate, s.bitrate_kbps);
                pushRing(h.latency, s.segment_ms_p95);
                const state = s.on_placeholder ? 'No Signal' : (s.exhausted ? 'Exhausted' : (s.state === 'legacy' ? 'Legacy proxy' : (s.flowing ? 'Live' : s.state)));
                const edgeBad = typeof s.seconds_since_segment === 'number' && s.seconds_since_segment > 15;
                return '<tr style="border-top: 1px solid var(--border);">' +
                    '<td style="padding: 0.4rem;">' + esc(s.name || id) + '</td>' +
                    '<td style="padding: 0.4rem;">' + esc(state) + (s.watched ? '' : ' <span style="color: var(--text-muted);">(unwatched)</span>') + '</td>' +
                    '<td style="padding: 0.4rem;">' + esc(fmt(s.bitrate_kbps, 'kbps')) + '<br>' + sparkline(h.bitrate) + '</td>' +
                    '<td style="padding: 0.4rem;">' + esc(fmt(s.segment_ms_p95, 'ms')) + '<br>' + sparkline(h.latency) + '</td>' +
                    '<td style="padding: 0.4rem;' + (edgeBad ? ' color: var(--danger);' : '') + '">' + esc(fmt(s.seconds_since_segment, 's', 1)) + '</td>' +
                    '<td style="padding: 0.4rem;">' + esc(fmt(s.memory_mb, 'MB', 1)) + '</td>' +
                    '<td style="padding: 0.4rem;">' + esc(s.failover_count ?? 0) + '</td></tr>';
            }).join('');
        }
        const br = document.getElementById('engine-breakers');
        if (br && status) {
            const names = Object.keys(status.breakers || {}).sort();
            br.innerHTML = names.length ? 'Provider breakers: ' + names.map(n => {
                const b = status.breakers[n];
                return esc(n) + ' ' + (b.open ? '<strong style="color: var(--danger);">open</strong>' : 'closed') + (b.failures ? ' (' + esc(b.failures) + ' failures)' : '');
            }).join(' &middot; ') : '';
        }
    }

    function renderLegacy(status) {
        const body = document.getElementById('engine-legacy-body');
        if (!body || !status) return;
        const reasons = status.legacy_reasons || {};
        const rows = status.legacy_fallbacks || [];
        body.innerHTML = rows.length ? rows.map(r =>
            '<tr><td style="padding: 0.4rem;">' + esc(reasons[r.reason] || r.reason) + '</td><td style="padding: 0.4rem;">' + esc(r.provider) + '</td><td style="padding: 0.4rem;">' + esc(r.count) + '</td></tr>'
        ).join('') : '<tr><td colspan="3" style="padding: 0.5rem; color: var(--text-muted);">No fallbacks since start.</td></tr>';
    }

    async function refresh() {
        if (document.hidden || !document.getElementById('engine-sessions-body')) return;
        try {
            const [sessions, status] = await Promise.all([
                fetch('/api/sessions').then(r => r.json()),
                fetch('/api/engine/status').then(r => r.json()),
            ]);
            render(sessions.sessions || {}, status);
            renderLegacy(status);
        } catch (e) {
            console.error('Live sessions refresh failed', e);
        }
    }

    window.JellyballEngine = {pushRing, sparkline};
    document.addEventListener('DOMContentLoaded', () => {
        refresh();
        setInterval(refresh, 3000);
    });
})();
