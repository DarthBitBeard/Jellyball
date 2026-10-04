function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'})[c]);
}
function getToastContainer() {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        container.setAttribute('role', 'status');
        container.setAttribute('aria-live', 'polite');
        document.body.appendChild(container);
    }
    return container;
}
function showToast(message, duration = 3000, isError = false) {
    const toast = document.createElement('div');
    toast.className = 'toast' + (isError ? ' error' : '');
    toast.textContent = message;
    getToastContainer().appendChild(toast);
    setTimeout(() => {
        toast.style.animation = 'slideOut 0.3s ease-out forwards';
        setTimeout(() => toast.remove(), 300);
    }, duration);
}
function copyToClipboard(text, message = 'Copied!') {
    navigator.clipboard.writeText(text).then(() => {
        showToast(message);
    }).catch(() => {
        showToast('Failed to copy', 3000, true);
    });
}
function toggleTheme() {
    const html = document.documentElement;
    const isDark = html.getAttribute('data-theme') !== 'light';
    const newTheme = isDark ? 'light' : 'dark';
    html.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    const btn = document.getElementById('theme-toggle');
    btn.textContent = isDark ? '☀️' : '🌙';
    btn.classList.remove('spin');
    void btn.offsetWidth;
    btn.classList.add('spin');
}
function switchTab(tabName) {
    document.querySelectorAll('.tab-pane').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
    const pane = document.getElementById('tab-' + tabName);
    const btn = document.getElementById('btn-' + tabName);
    if (pane) pane.classList.add('active');
    if (btn) btn.classList.add('active');
    if (tabName === 'logs') loadLogs();
    if (tabName === 'performance' && window.refreshPerformanceTab) window.refreshPerformanceTab(true);
    const url = new URL(window.location);
    url.searchParams.set('tab', tabName);
    window.history.replaceState({}, '', url);
}

let _lastLogLines = [];
let _logAutoRefreshTimer = null;

function _classifyLogLine(line) {
    if (/\bERROR\b/.test(line)) return 'log-line-error';
    if (/\bWARNING\b/.test(line)) return 'log-line-warning';
    return '';
}
function renderLogLines() {
    const viewer = document.getElementById('log-viewer');
    if (!viewer) return;
    const filterEl = document.getElementById('log-filter');
    const query = filterEl ? filterEl.value.toLowerCase().trim() : '';
    const lines = query
        ? _lastLogLines.filter(line => line.toLowerCase().includes(query))
        : _lastLogLines;
    if (!lines.length) {
        viewer.textContent = _lastLogLines.length ? 'No log lines match the filter.' : 'No log entries available.';
        return;
    }
    viewer.innerHTML = lines.map(line => {
        const cls = _classifyLogLine(line);
        return cls ? `<span class="${cls}">${escapeHtml(line)}</span>` : escapeHtml(line);
    }).join('\n');
    viewer.scrollTop = viewer.scrollHeight;
}
async function loadLogs() {
    const viewer = document.getElementById('log-viewer');
    if (!viewer) return;
    viewer.textContent = 'Loading logs...';
    try {
        const response = await fetch('/api/logs?limit=500', { cache: 'no-store' });
        if (!response.ok) throw new Error('Unable to load logs');
        const payload = await response.json();
        _lastLogLines = payload.logs || [];
        renderLogLines();
    } catch (error) {
        viewer.textContent = 'Unable to load logs. Check the application log file directly.';
    }
}
function toggleLogAutoRefresh(enabled) {
    if (_logAutoRefreshTimer) {
        window.clearInterval(_logAutoRefreshTimer);
        _logAutoRefreshTimer = null;
    }
    if (enabled) {
        _logAutoRefreshTimer = window.setInterval(loadLogs, 5000);
    }
}
function filterCatalog(value) {
    const query = (value || '').toLowerCase().trim();
    document.querySelectorAll('.catalog-group').forEach(group => {
        let visible = 0;
        group.querySelectorAll('.catalog-item').forEach(item => {
            const matches = !query || (item.dataset.catalogName || '').toLowerCase().includes(query);
            item.style.display = matches ? '' : 'none';
            if (matches) visible += 1;
        });
        group.style.display = visible ? '' : 'none';
        if (query && visible) group.open = true;
    });
}
function filterChannels(value) {
    const query = (value || '').toLowerCase().trim();
    document.querySelectorAll('.channel-card').forEach(card => {
        const matches = !query || (card.dataset.channelName || '').toLowerCase().includes(query);
        card.style.display = matches ? '' : 'none';
    });
}
function updateCatalogSelectionCount() {
    const selected = document.querySelectorAll('#catalog-form input[name="catalog_keys"]:checked').length;
    const label = document.getElementById('catalog-selection-count');
    if (label) label.textContent = 'Selected: ' + selected;
}

function _isUserInteracting() {
    // Don't yank the page out from under someone who has an open picker, a
    // focused form field, or unsaved text in the channel filter box.
    const active = document.activeElement;
    if (active) {
        const tag = active.tagName;
        if (tag === 'INPUT' || tag === 'SELECT' || tag === 'TEXTAREA') return true;
    }
    if (document.querySelector('details[open]')) return true;
    const filterEl = document.getElementById('channel-filter');
    if (filterEl && filterEl.value.trim()) return true;
    return false;
}
function _showChannelsChangedNotice() {
    const notice = document.getElementById('channels-changed-notice');
    if (notice) notice.style.display = 'block';
}
function applyStatusSnapshot(snapshot) {
    // Structural changes (a channel was added/removed, e.g. from another
    // tab or a scheduled auto-disable) still need a reload since we don't
    // have the HTML to insert a brand-new card client-side. Everything
    // else — candidate count, active provider, health — is common during
    // normal failover/rescrape churn and is patched into the existing
    // card in place instead, so the page doesn't jump/reload every ~5s.
    const channels = snapshot.channels || [];
    const byId = new Map(channels.map(channel => [channel.team_id, channel]));
    let shouldReload = channels.length !== document.querySelectorAll('.channel-card').length;
    document.querySelectorAll('.channel-card').forEach(card => {
        const channel = byId.get(card.dataset.teamId);
        if (!channel) {
            shouldReload = true;
            return;
        }
        const candidateCount = Number(channel.candidate_count || 0);
        const activeProvider = channel.active_provider || '';
        card.dataset.candidateCount = String(candidateCount);
        card.dataset.activeProvider = activeProvider;
        const dot = card.querySelector('[data-role="status-dot"]');
        const statusText = card.querySelector('[data-role="status-text"]');
        const candidateText = card.querySelector('[data-role="candidate-count"]');
        const watchingBadge = card.querySelector('[data-role="watching-badge"]');
        if (dot) {
            dot.classList.toggle('online', Boolean(channel.healthy));
            dot.classList.toggle('offline', !channel.healthy);
        }
        if (statusText) {
            if (channel.healthy) {
                statusText.textContent = 'Stream Stable & Active';
            } else if (channel.schedule_status === 'off_season') {
                statusText.textContent = channel.season_resume_label
                    ? `Off-season (resumes ${channel.season_resume_label})`
                    : 'Off-season';
            } else {
                statusText.textContent = 'Searching / Re-evaluating';
            }
        }
        if (candidateText) candidateText.textContent = 'Backups: ' + candidateCount;
        if (watchingBadge) {
            if (channel.watching) {
                watchingBadge.style.display = '';
                watchingBadge.className = 'badge ' + (channel.on_placeholder ? 'badge-no-signal' : 'badge-watching');
                watchingBadge.textContent = channel.on_placeholder
                    ? `⛔ No Signal (${channel.failover_count || 0} failover${channel.failover_count === 1 ? '' : 's'})`
                    : '▶ Watching';
            } else {
                watchingBadge.style.display = 'none';
                watchingBadge.textContent = '';
            }
        }
    });
    if (shouldReload) {
        if (_isUserInteracting()) {
            _showChannelsChangedNotice();
        } else {
            window.location.reload();
        }
    }
}
async function pollChannelStatus() {
    try {
        const response = await fetch('/api/status', { cache: 'no-store' });
        if (response.ok) applyStatusSnapshot(await response.json());
    } catch (error) {
        // The next poll retries transient server or network failures.
    } finally {
        window.setTimeout(pollChannelStatus, 5000);
    }
}
function applyMultiviewStatusSnapshot(snapshot) {
    const byId = new Map((snapshot.channels || []).map(ch => [ch.channel_id, ch]));
    document.querySelectorAll('.multiview-card').forEach(card => {
        const ch = byId.get(card.dataset.teamId);
        if (!ch) return;
        const statusEl = card.querySelector('[data-role="mv-status"]');
        const failureEl = card.querySelector('[data-role="mv-failure"]');
        if (statusEl) statusEl.textContent = ch.running ? '🟢 Running' : '⚪ Stopped';
        if (failureEl) {
            if (!ch.running && ch.last_error) {
                failureEl.innerHTML = `<p class="meta-text" style="color:var(--danger-text);">⚠️ ${escapeHtml(ch.last_error)} (failed ${escapeHtml(ch.failure_count)}x, retrying in ${escapeHtml(ch.retry_in_seconds)}s)</p>`;
            } else {
                failureEl.innerHTML = '';
            }
        }
    });
}
async function pollMultiviewStatus() {
    if (!document.querySelector('.multiview-card')) {
        window.setTimeout(pollMultiviewStatus, 5000);
        return;
    }
    try {
        const response = await fetch('/api/ffmpeg-status', { cache: 'no-store' });
        if (response.ok) applyMultiviewStatusSnapshot(await response.json());
    } catch (error) {
        // The next poll retries transient server or network failures.
    } finally {
        window.setTimeout(pollMultiviewStatus, 5000);
    }
}
function updateBulkTeamIds() {
    const checked = document.querySelectorAll('.team-bulk-select:checked');
    const ids = Array.from(checked).map(c => c.dataset.teamId).join(',');
    document.getElementById('bulk-team-ids').value = ids;
    return ids;
}
function updateMultiviewTeamIds() {
    const checked = document.querySelectorAll('.multiview-member-select:checked');
    if (checked.length !== 2 && checked.length !== 4) {
        showToast('Select exactly 2 or 4 channels for a Multi-View', 3000, true);
        return false;
    }
    document.getElementById('multiview-team-ids').value = Array.from(checked).map(c => c.dataset.teamId).join(',');
    return true;
}
function selectAllTeams() {
    document.querySelectorAll('.team-bulk-select').forEach(c => c.checked = true);
    updateBulkTeamIds();
    showToast(`Selected ${document.querySelectorAll('.team-bulk-select:checked').length} teams`);
}
function deselectAllTeams() {
    document.querySelectorAll('.team-bulk-select').forEach(c => c.checked = false);
    updateBulkTeamIds();
    showToast('Deselected all teams');
}
function bulkFavorite() {
    const ids = updateBulkTeamIds();
    if (!ids) {
        showToast('Select teams first', 3000, true);
        return;
    }
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = '/bulk-favorite';
    form.innerHTML = `<input type="hidden" name="team_ids" value="${escapeHtml(ids)}">`;
    document.body.appendChild(form);
    form.submit();
}
function bulkUnfavorite() {
    const ids = updateBulkTeamIds();
    if (!ids) {
        showToast('Select teams first', 3000, true);
        return;
    }
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = '/bulk-unfavorite';
    form.innerHTML = `<input type="hidden" name="team_ids" value="${escapeHtml(ids)}">`;
    document.body.appendChild(form);
    form.submit();
}
function bulkRemove() {
    const ids = updateBulkTeamIds();
    if (!ids) {
        showToast('Select teams first', 3000, true);
        return;
    }
    if (!confirm(`Remove ${document.querySelectorAll('.team-bulk-select:checked').length} teams?`)) return;
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = '/bulk-remove';
    form.innerHTML = `<input type="hidden" name="team_ids" value="${escapeHtml(ids)}">`;
    document.body.appendChild(form);
    form.submit();
}
async function testTeamStream(teamId) {
    const card = document.querySelector(`.channel-card[data-team-id="${teamId}"]`);
    const resultEl = card ? card.querySelector('[data-role="test-result"]') : null;
    if (resultEl) {
        resultEl.textContent = 'Testing...';
        resultEl.style.color = 'var(--text-muted)';
    }
    try {
        const resp = await fetch(`/api/test-stream/${teamId}`, { method: 'POST' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const result = await resp.json();
        if (resultEl) {
            const count = result.candidate_count || 0;
            resultEl.textContent = `${result.status} (${count} candidate${count === 1 ? '' : 's'})`;
            resultEl.style.color = result.is_live ? '#22c55e' : 'var(--danger-text)';
        }
    } catch (err) {
        if (resultEl) {
            resultEl.textContent = '❌ Test failed';
            resultEl.style.color = 'var(--danger-text)';
        }
    }
}
function importConfig(file) {
    if (!file) return;
    const reader = new FileReader();
    reader.onload = async (e) => {
        try {
            const config = JSON.parse(e.target.result);
            const resp = await fetch('/api/import-config', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(config)
            });
            if (resp.ok) {
                const result = await resp.json();
                showToast(`✅ Imported ${result.count} teams`);
                setTimeout(() => window.location.reload(), 2000);
            } else {
                showToast('Import failed', 3000, true);
            }
        } catch (error) {
            showToast('Invalid config file', 3000, true);
        }
    };
    reader.readAsText(file);
}

window.addEventListener('DOMContentLoaded', () => {
    const params = new URLSearchParams(window.location.search);
    const activeTab = params.get('tab');
    const status = params.get('status');

    if (activeTab && document.getElementById('tab-' + activeTab)) switchTab(activeTab);
    if (activeTab === 'logs') loadLogs();
    updateCatalogSelectionCount();
    pollChannelStatus();
    pollMultiviewStatus();

    const statusMessages = {
        'jellyfin_success': '✅ Jellyfin connection successful!',
        'jellyfin_failed': '❌ Jellyfin connection failed',
        'jellyfin_failed_config': '❌ Enter the Jellyfin URL and API key first',
        'jellyfin_failed_unreachable': '❌ Cannot reach Jellyfin - check the server URL and that Jellyfin is running',
        'jellyfin_failed_auth': '❌ Jellyfin rejected the API key',
        'jellyfin_failed_task': '❌ Connected, but no Refresh Guide task was found - is Live TV set up in Jellyfin?',
        'jellyfin_key_required': '❌ Re-enter the API key when changing the Jellyfin server',
        'jellyfin_saved': '✅ Jellyfin settings saved',
        'team_added': '✅ Channel added',
        'team_removed': '✅ Channel removed',
        'schedule_saved': '✅ Auto-disable date saved',
        'schedule_invalid': '❌ Enter a valid date (YYYY-MM-DD)',
        'schedule_failed': '❌ Could not save the auto-disable date',
        'jellyfin_url_invalid': '❌ Jellyfin URL must be an absolute http(s) URL',
        'saved': '✅ Settings saved',
        'override_saved': '✅ Manual override applied',
        'test_sent': '✅ Test alert sent',
        'providers_saved': '✅ Provider domains saved',
        'providers_invalid': '❌ One or more provider URLs must be an absolute http(s) URL',
        'catalog_applied': '✅ Catalog selection applied',
        'catalog_toggled': '✅ Catalog entry updated',
        'favorite_toggled': '✅ Favorite updated',
        'rescrape_started': '✅ Rescrape started',
        'bulk_favorited': '✅ Favorited selected teams',
        'bulk_unfavorited': '✅ Unfavorited selected teams',
        'bulk_removed': '✅ Removed selected teams',
        'multiview_created': '✅ Multi-View created',
        'multiview_removed': '✅ Multi-View removed',
        'multiview_stopped': '✅ Multi-View stopped',
        'multiview_started': '✅ Multi-View started',
        'multiview_audio_set': '✅ Multi-View audio updated',
        'advanced_saved': '✅ Advanced settings saved',
        'advanced_invalid': '❌ A value was not a number; nothing after it was saved',
    };
    if (status && statusMessages[status]) {
        showToast(statusMessages[status], 4000, status.includes('failed') || status.includes('invalid'));
        const statusEl = document.getElementById('jellyfin-status');
        if (statusEl && status.includes('jellyfin')) {
            statusEl.textContent = statusMessages[status];
            statusEl.style.color = status.includes('success') || status.includes('saved') ? 'var(--success)' : 'var(--danger)';
        }
        const newUrl = new URL(window.location);
        newUrl.searchParams.delete('status');
        window.history.replaceState({}, '', newUrl);
    }

    const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
    document.getElementById('theme-toggle').textContent = currentTheme === 'light' ? '☀️' : '🌙';

    const overrideSelect = document.getElementById('global-provider-override');
    if (overrideSelect) {
        overrideSelect.addEventListener('change', (e) => {
            sessionStorage.setItem('provider-override', e.target.value);
            showToast(e.target.value ? `Provider override set to: ${e.target.value}` : 'Provider override cleared');
            if (window.Jellyfin) {
                window.Jellyfin.mediaManager?.seekTo?.(0);
            }
        });
        const saved = sessionStorage.getItem('provider-override');
        if (saved) overrideSelect.value = saved;
    }

    if (typeof MediaSession !== 'undefined') {
        navigator.mediaSession.setActionHandler('play', () => {});
        navigator.mediaSession.setActionHandler('pause', () => {});
    }

    document.querySelectorAll('.team-bulk-select').forEach(checkbox => {
        checkbox.addEventListener('change', updateBulkTeamIds);
    });

    const logAutoRefreshToggle = document.getElementById('log-autorefresh');
    if (logAutoRefreshToggle) {
        logAutoRefreshToggle.addEventListener('change', (e) => toggleLogAutoRefresh(e.target.checked));
    }
    const logFilterInput = document.getElementById('log-filter');
    if (logFilterInput) {
        logFilterInput.addEventListener('input', renderLogLines);
    }

    // The Performance tab refreshes whenever it is the visible tab, including after the
    // operator switches to it (the page may have loaded on another tab).
    {
        const performancePaneActive = () => {
            const pane = document.getElementById('tab-performance');
            return !!pane && pane.classList.contains('active');
        };
        async function refreshPerformanceTab(withTopTeams) {
            if (!document.getElementById('performance-metrics')) return;
            try {
                const cache = await fetch('/api/cache-metrics').then(r => r.json());
                const perf = await fetch('/api/performance-stats').then(r => r.json());

                const cacheEl = document.getElementById('cache-hit-rate');
                const playEl = document.getElementById('playback-sessions');
                const failEl = document.getElementById('failover-count');
                const dbEl = document.getElementById('db-size');
                const rateLimitEl = document.getElementById('rate-limiting-status');

                if (cacheEl) cacheEl.textContent = (cache.hit_rate || 0).toFixed(1) + '%';
                if (playEl) playEl.textContent = perf.playback_sessions_hour || 0;
                if (failEl) failEl.textContent = perf.failovers_hour || 0;
                if (dbEl) dbEl.textContent = (perf.db_size_mb || 0).toFixed(1) + ' MB';

                if (rateLimitEl) {
                    const health = perf.provider_health || [];
                    if (!health.length) {
                        rateLimitEl.innerHTML = '<div style="background: var(--surface-2); padding: 0.75rem; border-radius: 6px; border: 1px solid var(--border); color: var(--text-muted);">Not enough recent provider activity yet.</div>';
                    } else {
                        rateLimitEl.innerHTML = health.map(p => {
                            if (p.sustained_failure) {
                                return `<div style="background: var(--surface-2); padding: 0.75rem; border-radius: 6px; border: 1px solid var(--danger);"><div style="font-weight:600;">${escapeHtml(p.provider)}</div><div style="color: var(--danger);">⛔ Likely dead — 0% over ${p.samples_5d} attempts/5d</div></div>`;
                            }
                            const color = p.at_risk ? 'var(--danger)' : 'var(--success)';
                            const icon = p.at_risk ? '⚠️' : '✅';
                            const rateText = p.success_rate === null ? 'no data this hour' : `${p.success_rate}% (${p.samples_hour} samples/hr)`;
                            return `<div style="background: var(--surface-2); padding: 0.75rem; border-radius: 6px; border: 1px solid var(--border);"><div style="font-weight:600;">${escapeHtml(p.provider)}</div><div style="color:${color};">${icon} ${escapeHtml(rateText)}</div></div>`;
                        }).join('');
                    }
                }

                if (withTopTeams) {
                    const playback = await fetch('/api/playback-stats').then(r => r.json());
                    const topEl = document.getElementById('top-teams-list');
                    if (topEl && playback.top_watched_teams) {
                        topEl.innerHTML = playback.top_watched_teams.map(t => `<div style="padding: 0.5rem; background: var(--surface-2); border-radius: 6px; display: flex; justify-content: space-between;"><span>${escapeHtml(t.team_id)}</span><span style="color: var(--success);">${escapeHtml(t.plays)} plays</span></div>`).join('');
                    }
                }
            } catch (e) {
                console.error('Performance load failed', e);
            }
        }
        window.refreshPerformanceTab = refreshPerformanceTab;
        if (performancePaneActive()) refreshPerformanceTab(true);
        setInterval(() => { if (performancePaneActive() && !document.hidden) refreshPerformanceTab(false); }, 5000);
    }
});
