// Provider Status card (Performance tab): per-provider health, a dry-run test
// button, and enable/priority controls. Uses escapeHtml/showToast from dashboard.js.
(function () {
    const card = document.getElementById('provider-status-card');
    const list = document.getElementById('provider-status-list');
    if (!card || !list) return;

    const esc = (value) => (typeof escapeHtml === 'function' ? escapeHtml(value) : String(value));
    const testResults = {};
    let rows = [];

    function relativeTime(value) {
        if (!value) return 'never';
        const text = String(value);
        const when = new Date(text.replace(' ', 'T') + (text.includes('Z') ? '' : 'Z'));
        if (isNaN(when)) return esc(value);
        const seconds = Math.max(0, Math.round((Date.now() - when.getTime()) / 1000));
        if (seconds < 90) return 'just now';
        if (seconds < 5400) return Math.round(seconds / 60) + ' min ago';
        if (seconds < 172800) return Math.round(seconds / 3600) + ' h ago';
        return Math.round(seconds / 86400) + ' d ago';
    }

    function isNil(value) {
        return value === null || value === undefined;
    }

    function testLine(test) {
        if (test === 'running') return '<div class="hint-text">Testing...</div>';
        if (!test) return '';
        if (test.error) return `<div style="color: var(--danger-text);">${esc(test.error)}</div>`;
        const klass = test.error_class ? ' (' + esc(test.error_class) + ')' : '';
        const events = isNil(test.index_events) ? 'index unknown' : esc(test.index_events) + ' events listed';
        return `<div class="hint-text">Test: ${esc(test.outcome)}${klass}, ${events}, ${esc(test.matches)} matched, ${esc(test.streams)} stream(s), ${esc(test.response_time_ms)} ms</div>`;
    }

    function rowHtml(p) {
        const breaker = p.breaker_open
            ? `<span style="color: var(--danger);">breaker open (${esc(p.breaker_failures)} failures)</span>`
            : '<span style="color: var(--success);">breaker closed</span>';
        const events = isNil(p.index_events)
            ? 'unknown'
            : (p.index_events === 0 ? '<span style="color: var(--danger);">0</span>' : esc(p.index_events));
        const last = p.last_outcome
            ? esc(p.last_outcome) + (p.last_error_class ? ' (' + esc(p.last_error_class) + ')' : '')
            : 'not run yet';
        const placeholder = isNil(p.default_priority) ? 'auto' : esc(p.default_priority);
        const value = isNil(p.priority) ? '' : esc(p.priority);
        return `<div style="background: var(--surface-2); padding: 0.75rem; border-radius: 8px; border: 1px solid var(--border); ${p.enabled ? '' : 'opacity: 0.6;'}" data-provider="${esc(p.name)}">
            <div style="display: flex; justify-content: space-between; align-items: center; gap: 0.5rem; flex-wrap: wrap;">
                <strong>${esc(p.name)}</strong>
                <span>
                    <label style="margin-right: 0.5rem;"><input type="checkbox" data-role="enabled" ${p.enabled ? 'checked' : ''}> enabled</label>
                    <label style="margin-right: 0.5rem;">priority <input type="number" data-role="priority" min="0" max="999" style="width: 4.5rem;" placeholder="${placeholder}" value="${value}"></label>
                    <button type="button" class="btn-secondary" data-role="test">Test</button>
                </span>
            </div>
            <div class="hint-text">Last success: ${relativeTime(p.last_success)} &middot; index events: ${events} &middot; last search: ${last} &middot; ${breaker}</div>
            ${testLine(testResults[p.name])}
        </div>`;
    }

    function render() {
        // Don't clobber a priority field the operator is typing in.
        if (list.contains(document.activeElement) && document.activeElement.dataset.role === 'priority') return;
        list.innerHTML = rows.length ? rows.map(rowHtml).join('') : '<p style="color: var(--text-muted);">No providers.</p>';
    }

    async function refresh() {
        try {
            const resp = await fetch('/api/providers');
            if (!resp.ok) return;
            rows = (await resp.json()).providers || [];
            render();
        } catch (e) { /* keep the last render */ }
    }

    function paneActive() {
        const pane = document.getElementById('tab-performance');
        return !!pane && pane.classList.contains('active') && !document.hidden;
    }

    async function saveSettings(name, row) {
        const enabled = row.querySelector('[data-role="enabled"]').checked;
        const raw = row.querySelector('[data-role="priority"]').value.trim();
        const parsed = raw === '' ? null : parseInt(raw, 10);
        const resp = await fetch(`/api/providers/${encodeURIComponent(name)}/settings`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ enabled, priority: Number.isNaN(parsed) ? null : parsed }),
        });
        if (typeof showToast === 'function') showToast(resp.ok ? name + ' updated' : 'Could not update ' + name, 2500, !resp.ok);
        if (document.activeElement && document.activeElement.blur) document.activeElement.blur();
        refresh();
    }

    async function runTest(name) {
        testResults[name] = 'running';
        render();
        try {
            const resp = await fetch(`/api/providers/${encodeURIComponent(name)}/test`, { method: 'POST' });
            testResults[name] = resp.ok ? await resp.json() : { error: resp.status === 429 ? 'A test is already running' : 'Test failed' };
        } catch (e) {
            testResults[name] = { error: 'Test failed' };
        }
        render();
    }

    list.addEventListener('change', (event) => {
        const role = event.target.dataset ? event.target.dataset.role : '';
        const row = event.target.closest('[data-provider]');
        if (row && (role === 'enabled' || role === 'priority')) saveSettings(row.dataset.provider, row);
    });
    list.addEventListener('click', (event) => {
        const button = event.target.closest('[data-role="test"]');
        const row = button && button.closest('[data-provider]');
        if (row) runTest(row.dataset.provider);
    });
    document.addEventListener('click', (event) => {
        if (event.target.closest && event.target.closest('#btn-performance')) refresh();
    });

    try { rows = JSON.parse(card.dataset.initial || '[]'); } catch (e) { rows = []; }
    render();
    setInterval(() => { if (paneActive()) refresh(); }, 10000);
    if (paneActive()) refresh();
})();
