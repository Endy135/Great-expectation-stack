// ─────────────────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────────────────

const API = window.location.port === '8080' ? '/api' : 'http://localhost:5000/api';

const TAB_TITLES = {
  data:     'Aperçu des données',
  suites:   'Suites d\'expectations',
  validate: 'Validation',
  results:  'Résultats',
  docs:     'Documentation API',
};

// ─────────────────────────────────────────────────────────────────────────────
// NAVIGATION
// ─────────────────────────────────────────────────────────────────────────────

function goto(tab, event) {
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  if (event) event.currentTarget.classList.add('active');
  document.querySelectorAll('.tab-panel').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  document.getElementById('topbar-title').textContent = TAB_TITLES[tab] || '';

  if (tab === 'suites')   { loadCatalogue(); loadSuites(); }
  if (tab === 'validate') loadValidateSuites();
  if (tab === 'results')  loadResults();
}

// ─────────────────────────────────────────────────────────────────────────────
// TOAST
// ─────────────────────────────────────────────────────────────────────────────

let _toastTimer;
function toast(msg, type = 'info') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.style.borderColor = type === 'danger' ? 'var(--danger)' : type === 'success' ? 'var(--success)' : 'var(--border2)';
  el.classList.add('show');
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => el.classList.remove('show'), 3000);
}

// ─────────────────────────────────────────────────────────────────────────────
// API HEALTH
// ─────────────────────────────────────────────────────────────────────────────

async function checkHealth() {
  try {
    const r = await fetch(`${API}/health`, { signal: AbortSignal.timeout(5000) });
    const ok = r.ok;
    document.getElementById('api-dot').className = 'dot ' + (ok ? 'ok' : 'err');
    document.getElementById('api-status-text').textContent = ok ? 'API connectée' : 'Erreur API';
    return ok;
  } catch {
    document.getElementById('api-dot').className = 'dot err';
    document.getElementById('api-status-text').textContent = 'API hors ligne';
    return false;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SOURCE MANAGER — Local / MinIO / PostgreSQL
// ─────────────────────────────────────────────────────────────────────────────

let currentSource = 'local';

async function switchSource(type) {
  if (type === currentSource) return;
  const r = await fetch(`${API}/source/switch`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type }),
  });
  if (!r.ok) { toast(`Erreur switch source : ${r.status}`, 'danger'); return; }
  currentSource = type;
  _updateSourceTabs(type);
  _showSourcePanel(type);
  await refreshSource();
}

function _updateSourceTabs(active) {
  ['local', 'minio', 'postgres'].forEach(t => {
    const btn = document.getElementById(`src-btn-${t}`);
    if (!btn) return;
    btn.classList.toggle('source-tab-active', t === active);
  });
}

function _showSourcePanel(type) {
  ['local', 'minio', 'postgres'].forEach(t => {
    const el = document.getElementById(`source-panel-${t}`);
    if (el) el.style.display = t === type ? '' : 'none';
  });
  const meta = document.getElementById('source-meta');
  if (meta) { meta.style.display = 'none'; meta.innerHTML = ''; }
}

async function refreshSource() {
  if (currentSource === 'local')    return loadFiles();
  if (currentSource === 'minio')    return loadMinioBuckets();
  if (currentSource === 'postgres') return loadPgTables();
}

// ── LOCAL ─────────────────────────────────────────────────────────────────────

async function loadFiles() {
  try {
    const resp = await fetch(`${API}/data/files`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data  = await resp.json();
    const files = data.files || [];
    const sel   = document.getElementById('file-select');
    if (!sel) return;
    if (!files.length) {
      sel.innerHTML = '<option value="">— Aucun fichier dans /data —</option>';
      return;
    }
    sel.innerHTML = files.map(f =>
      `<option value="${f.name}" ${f.active ? 'selected' : ''}>${f.name} (${f.size_kb} KB)</option>`
    ).join('');
    const active = files.find(f => f.active) || files[0];
    _setSourceMeta('local', active);
  } catch (e) {
    const sel = document.getElementById('file-select');
    if (sel) sel.innerHTML = '<option>Erreur de chargement</option>';
  }
}

async function selectFile() {
  const name = document.getElementById('file-select')?.value;
  if (!name) return;
  try {
    const r = await fetch(`${API}/data/select`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({ error: `HTTP ${r.status}` }));
      toast(`Erreur : ${err.error || r.status}`, 'danger');
      return;
    }
    toast(`Fichier actif : ${name}`, 'success');
    await refreshColumns();
    renderExpParams();
    await loadData();
  } catch (e) {
    toast('Erreur : ' + e.message, 'danger');
  }
}

// ── MINIO ──────────────────────────────────────────────────────────────────────

async function loadMinioBuckets() {
  const sel = document.getElementById('minio-bucket-select');
  const err = document.getElementById('minio-error');
  if (!sel) return;
  try {
    const r = await fetch(`${API}/minio/buckets`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data    = await r.json();
    const buckets = data.buckets || [];
    if (err) err.style.display = 'none';
    if (!buckets.length) {
      sel.innerHTML = '<option value="">— Aucun bucket —</option>';
      return;
    }
    sel.innerHTML = buckets.map(b => `<option value="${b}">${b}</option>`).join('');
    await loadMinioFiles();
  } catch (e) {
    if (err) { err.textContent = `MinIO inaccessible : ${e.message}`; err.style.display = 'block'; }
    if (sel) sel.innerHTML = '<option value="">Erreur</option>';
  }
}

async function loadMinioFiles() {
  const bucketSel = document.getElementById('minio-bucket-select');
  const fileSel   = document.getElementById('minio-file-select');
  const err       = document.getElementById('minio-error');
  if (!bucketSel || !fileSel) return;
  const bucket = bucketSel.value;
  if (!bucket) return;
  try {
    const r = await fetch(`${API}/minio/files?bucket=${encodeURIComponent(bucket)}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data  = await r.json();
    const files = data.files || [];
    if (err) err.style.display = 'none';
    if (!files.length) {
      fileSel.innerHTML = '<option value="">— Aucun fichier supporté —</option>';
      return;
    }
    fileSel.innerHTML = files.map(f =>
      `<option value="${f.name}" data-bucket="${f.bucket}" ${f.active ? 'selected' : ''}>${f.name} (${f.size_kb} KB)</option>`
    ).join('');
    const active = files.find(f => f.active);
    if (active) _setSourceMeta('minio', active);
  } catch (e) {
    if (err) { err.textContent = `Erreur fichiers : ${e.message}`; err.style.display = 'block'; }
  }
}

async function selectMinioFile() {
  const fileSel   = document.getElementById('minio-file-select');
  const bucketSel = document.getElementById('minio-bucket-select');
  if (!fileSel || !bucketSel) return;
  const key    = fileSel.value;
  const bucket = bucketSel.value;
  if (!key) return;
  try {
    const r = await fetch(`${API}/minio/select`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key, bucket }),
    });
    if (!r.ok) {
      const d = await r.json().catch(() => ({}));
      toast(`Erreur MinIO : ${d.error || r.status}`, 'danger');
      return;
    }
    const opt = fileSel.options[fileSel.selectedIndex];
    _setSourceMeta('minio', { name: key, size_kb: opt.textContent.match(/\((.+) KB\)/)?.[1] });
    toast(`MinIO actif : ${key}`, 'success');
    await refreshColumns();
    renderExpParams();
    await loadData();
  } catch (e) {
    toast('Erreur : ' + e.message, 'danger');
  }
}

// ── POSTGRESQL ─────────────────────────────────────────────────────────────────

async function loadPgTables() {
  const sel = document.getElementById('pg-table-select');
  const err = document.getElementById('pg-error');
  if (!sel) return;
  try {
    const r = await fetch(`${API}/postgres/tables`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data   = await r.json();
    const tables = data.tables || [];
    if (err) err.style.display = 'none';
    if (!tables.length) {
      sel.innerHTML = '<option value="">— Aucune table —</option>';
      return;
    }
    sel.innerHTML = '<option value="">— choisir une table —</option>' +
      tables.map(t => `<option value="${t}">${t}</option>`).join('');
  } catch (e) {
    if (err) { err.textContent = `PostgreSQL inaccessible : ${e.message}`; err.style.display = 'block'; }
    if (sel) sel.innerHTML = '<option value="">Erreur connexion</option>';
  }
}

async function selectPgTable() {
  const table = document.getElementById('pg-table-select')?.value;
  if (!table) return;
  await _activatePg(table);
}

async function selectPgQuery() {
  const query = document.getElementById('pg-query-input')?.value?.trim();
  if (!query) { toast('Saisir une requête SQL', 'danger'); return; }
  await _activatePg(query);
}

async function _activatePg(tableOrQuery) {
  const err = document.getElementById('pg-error');
  try {
    const r = await fetch(`${API}/postgres/select`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table: tableOrQuery }),
    });
    if (!r.ok) {
      const d = await r.json().catch(() => ({}));
      if (err) { err.textContent = d.error || `HTTP ${r.status}`; err.style.display = 'block'; }
      return;
    }
    if (err) err.style.display = 'none';
    _setSourceMeta('postgres', { name: tableOrQuery });
    toast(`PostgreSQL actif : ${tableOrQuery}`, 'success');
    await refreshColumns();
    renderExpParams();
    await loadData();
  } catch (e) {
    if (err) { err.textContent = e.message; err.style.display = 'block'; }
  }
}

// ── META BADGE ─────────────────────────────────────────────────────────────────

const SOURCE_ICONS = { local: '📁', minio: '🪣', postgres: '🐘' };

function _setSourceMeta(type, item) {
  const meta = document.getElementById('source-meta');
  if (!meta || !item) return;
  const size = item.size_kb ? ` · ${item.size_kb} KB` : '';
  meta.innerHTML = `<span style="margin-right:6px;">${SOURCE_ICONS[type] || ''}</span>${item.name || ''}${size}`;
  meta.style.display = 'block';
}

// ─────────────────────────────────────────────────────────────────────────────
// DATA TAB
// ─────────────────────────────────────────────────────────────────────────────

async function loadData() {
  document.getElementById('preview-table-wrap').innerHTML = '<div class="spinner"></div>';
  document.getElementById('col-profile').innerHTML = '<div class="spinner"></div>';

  try {
    const safeJson = async (url) => {
      const r = await fetch(url);
      if (!r.ok) {
        const text = await r.text();
        throw new Error(`HTTP ${r.status} : ${text.slice(0, 120)}`);
      }
      return r.json();
    };

    const [prev, stats] = await Promise.all([
      safeJson(`${API}/data/preview`),
      safeJson(`${API}/data/stats`),
    ]);

    if (prev.error) {
      document.getElementById('preview-table-wrap').innerHTML =
        `<div class="empty"><div class="empty-text" style="color:var(--danger)">${prev.error}</div></div>`;
      return;
    }

    // Stats rapides
    document.getElementById('s-rows').textContent  = prev.total_rows ?? '—';
    document.getElementById('s-cols').textContent  = (prev.columns || []).length;
    const totalNulls = Array.isArray(stats) ? stats.reduce((a, s) => a + (s.nulls || 0), 0) : '—';
    document.getElementById('s-nulls').textContent = totalNulls;
    document.getElementById('s-file').textContent  = (prev.label || 'CSV').split('.').pop().toUpperCase();

    // Table preview
    if (prev.columns && prev.rows) {
      let html = '<table class="tbl"><thead><tr>';
      prev.columns.forEach(c => html += `<th>${c}</th>`);
      html += '</tr></thead><tbody>';
      prev.rows.forEach(row => {
        html += '<tr>';
        prev.columns.forEach(c => {
          const v = row[c];
          const isEmpty = v === null || v === undefined || v === '';
          html += `<td>${isEmpty ? '<span style="color:var(--danger);opacity:.6">null</span>' : v}</td>`;
        });
        html += '</tr>';
      });
      html += '</tbody></table>';
      document.getElementById('preview-table-wrap').innerHTML = html;
    }

    // Profil colonnes
    if (Array.isArray(stats)) {
      let html = '';
      stats.forEach(s => {
        const nullPct   = s.total ? Math.round(s.nulls   / s.total * 100) : 0;
        const uniquePct = s.total ? Math.round(s.unique / s.total * 100) : 0;
        html += `
          <div style="margin-bottom:14px;padding-bottom:14px;border-bottom:1px solid var(--border)">
            <div class="flex-center mb-10">
              <span style="font-weight:600;font-size:13px;">${s.column}</span>
              <span class="badge badge-info" style="margin-left:8px;">${s.type}</span>
              ${s.nulls > 0 ? `<span class="badge badge-danger" style="margin-left:4px;">${s.nulls} nuls</span>` : ''}
              <span style="margin-left:auto;font-size:11px;color:var(--muted);">${s.unique} valeurs uniques (${uniquePct}%)</span>
            </div>
            <div style="display:flex;gap:8px;align-items:center;">
              <div style="font-size:11px;color:var(--muted);width:60px">Nulls ${nullPct}%</div>
              <div class="progress-bar" style="flex:1"><div class="progress-fill" style="width:${nullPct}%;background:${nullPct > 20 ? 'var(--danger)' : 'var(--success)'};"></div></div>
            </div>
            ${s.min !== undefined ? `<div style="margin-top:6px;font-size:11px;font-family:var(--mono);color:var(--muted)">min: ${s.min} · max: ${s.max} · mean: ${s.mean?.toFixed(1)}</div>` : ''}
          </div>`;
      });
      document.getElementById('col-profile').innerHTML = html || '<div class="empty"><div class="empty-text">Aucune stat disponible</div></div>';
    }
  } catch (e) {
    document.getElementById('preview-table-wrap').innerHTML =
      `<div class="empty"><div class="empty-text" style="color:var(--danger)">Erreur de chargement : ${e.message}</div></div>`;
    document.getElementById('col-profile').innerHTML = '';
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// CATALOGUE & COLONNES
// ─────────────────────────────────────────────────────────────────────────────

let catalogue = {};
let availableColumns = [];

async function loadCatalogue() {
  try {
    catalogue = await fetch(`${API}/expectations/catalogue`).then(r => r.json());
    const sel = document.getElementById('exp-type');
    if (!sel) return;
    sel.innerHTML = Object.entries(catalogue).map(([k, v]) =>
      `<option value="${k}">${v.label}</option>`
    ).join('');
    renderExpParams();
  } catch {}
}

async function refreshColumns() {
  try {
    const r = await fetch(`${API}/data/columns`);
    if (!r.ok) return;
    const data = await r.json();
    availableColumns = (data.columns || []).map(c => c.name || c);
  } catch {}
}

function renderExpParams() {
  const type = document.getElementById('exp-type')?.value;
  const wrap = document.getElementById('exp-params-wrap');
  if (!wrap || !type || !catalogue[type]) return;

  const params = catalogue[type].params || [];
  wrap.innerHTML = params.map(p => {
    if (p === 'column') {
      const opts = availableColumns.map(c => `<option value="${c}">${c}</option>`).join('');
      return `<div class="form-col"><div class="form-label">Colonne</div><select class="input" id="param-column">${opts || '<option>—</option>'}</select></div>`;
    }
    if (p === 'value_set') {
      return `<div class="form-col"><div class="form-label">Valeurs (séparées par virgule)</div><input class="input" id="param-value_set" placeholder="val1, val2, val3"></div>`;
    }
    return `<div class="form-col"><div class="form-label">${p}</div><input class="input" id="param-${p}" placeholder="${p}"></div>`;
  }).join('');
}

// ─────────────────────────────────────────────────────────────────────────────
// SUITES
// ─────────────────────────────────────────────────────────────────────────────

let suites = {};
let selectedSuite = null;

function renderSuiteList() {
  const el = document.getElementById('suite-list');
  const names = Object.keys(suites);
  if (!names.length) {
    el.innerHTML = '<div class="empty"><div class="empty-text">Aucune suite</div></div>';
    return;
  }
  el.innerHTML = names.map(n => `
    <div class="suite-item ${selectedSuite === n ? 'selected' : ''}" onclick="selectSuite('${n}')">
      <div>
        <div class="suite-name">${n}</div>
        <div class="suite-meta">${(suites[n].expectations || []).length} expectation(s)</div>
      </div>
    </div>`
  ).join('');
}

async function loadSuites() {
  suites = await fetch(`${API}/suites`).then(r => r.json()).catch(() => ({}));
  renderSuiteList();
  loadValidateSuites();
}

function selectSuite(name) {
  selectedSuite = name;
  renderSuiteList();
  document.getElementById('suite-detail-title').textContent = name;
  document.getElementById('btn-delete-suite').style.display = '';
  document.getElementById('exp-add-form').style.display = '';
  document.getElementById('exp-sep').style.display = '';
  renderExpList();
}

function renderExpList() {
  const s   = suites[selectedSuite];
  const el  = document.getElementById('exp-list');
  if (!s || !s.expectations?.length) {
    el.innerHTML = '<div class="empty"><div class="empty-text">Aucune expectation</div></div>';
    return;
  }
  el.innerHTML = s.expectations.map((e, i) => `
    <div class="exp-item">
      <span class="exp-type">${e.expectation_type}</span>
      <span class="exp-kwargs">${JSON.stringify(e.kwargs)}</span>
      <button class="btn btn-danger btn-sm" onclick="deleteExpectation(${i})">✕</button>
    </div>`
  ).join('');
}

function showCreateSuite() { document.getElementById('suite-create-form').style.display = ''; }
function hideCreateSuite() { document.getElementById('suite-create-form').style.display = 'none'; }

async function createSuite() {
  const name = document.getElementById('new-suite-name').value.trim();
  if (!name) return;
  const r = await fetch(`${API}/suites`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
  });
  if (!r.ok) { const d = await r.json(); toast(d.error, 'danger'); return; }
  document.getElementById('new-suite-name').value = '';
  hideCreateSuite();
  await loadSuites();
  selectSuite(name);
  toast(`Suite "${name}" créée`, 'success');
}

async function deleteSuite() {
  if (!selectedSuite) return;
  if (!confirm(`Supprimer la suite "${selectedSuite}" ?`)) return;
  await fetch(`${API}/suites/${selectedSuite}`, { method: 'DELETE' });
  selectedSuite = null;
  document.getElementById('suite-detail-title').textContent = 'Sélectionner une suite';
  document.getElementById('btn-delete-suite').style.display = 'none';
  document.getElementById('exp-add-form').style.display = 'none';
  document.getElementById('exp-sep').style.display = 'none';
  document.getElementById('exp-list').innerHTML = '<div class="empty"><div class="empty-text">Aucune expectation</div></div>';
  await loadSuites();
  toast('Suite supprimée', 'success');
}

async function addExpectation() {
  if (!selectedSuite) return;
  const type   = document.getElementById('exp-type')?.value;
  const params = catalogue[type]?.params || [];
  const kwargs = {};
  for (const p of params) {
    const el = document.getElementById(`param-${p}`);
    if (el) kwargs[p] = el.value;
  }
  const r = await fetch(`${API}/suites/${selectedSuite}/expectations`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ expectation_type: type, kwargs }),
  });
  if (!r.ok) { const d = await r.json(); toast(d.error, 'danger'); return; }
  await loadSuites();
  selectSuite(selectedSuite);
  toast('Expectation ajoutée', 'success');
}

async function deleteExpectation(idx) {
  if (!selectedSuite) return;
  await fetch(`${API}/suites/${selectedSuite}/expectations/${idx}`, { method: 'DELETE' });
  await loadSuites();
  selectSuite(selectedSuite);
}

// ─────────────────────────────────────────────────────────────────────────────
// VALIDATION
// ─────────────────────────────────────────────────────────────────────────────

function loadValidateSuites() {
  const sel = document.getElementById('val-suite-select');
  if (!sel) return;
  const names = Object.keys(suites);
  if (!names.length) {
    sel.innerHTML = '<option value="">— Aucune suite —</option>';
    return;
  }
  sel.innerHTML = names.map(n => `<option value="${n}">${n}</option>`).join('');
}

async function runValidation() {
  const suite = document.getElementById('val-suite-select')?.value;
  if (!suite) { toast('Sélectionner une suite', 'danger'); return; }
  const wrap = document.getElementById('val-result');
  wrap.innerHTML = '<div class="spinner"></div> Validation en cours…';
  try {
    const r   = await fetch(`${API}/validate/${suite}`, { method: 'POST' });
    const res = await r.json();
    if (!r.ok) { wrap.innerHTML = `<div class="empty"><div class="empty-text" style="color:var(--danger)">${res.error}</div></div>`; return; }

    const rate    = res.summary.success_rate;
    const color   = rate === 100 ? 'var(--success)' : rate >= 80 ? 'var(--warn)' : 'var(--danger)';
    let html = `
      <div style="margin-bottom:12px;">
        <div class="flex-center" style="margin-bottom:8px;">
          <span style="font-weight:700;font-size:22px;color:${color}">${rate}%</span>
          <span style="margin-left:10px;color:var(--muted);font-size:13px;">${res.summary.passed}/${res.summary.total} passées · source: ${res.source_type} · ${res.source_label}</span>
        </div>
        <div class="progress-bar"><div class="progress-fill" style="width:${rate}%;background:${color};"></div></div>
      </div>`;

    if (res.warnings?.length) {
      html += `<div style="margin-bottom:10px;padding:10px;background:rgba(251,191,36,.08);border:1px solid rgba(251,191,36,.2);border-radius:7px;font-size:12px;color:var(--warn);">
        ⚠️ ${res.warnings.map(w => w.message).join('<br>')}
      </div>`;
    }

    res.results.forEach(r => {
      const ok = r.success;
      html += `<div class="result-row">
        <span class="badge ${ok ? 'badge-success' : 'badge-danger'}">${ok ? '✓' : '✗'}</span>
        <span class="exp-type" style="flex:1">${r.expectation_type}</span>
        <span class="exp-kwargs">${JSON.stringify(r.kwargs)}</span>
        ${r.error ? `<span style="color:var(--danger);font-size:11px">${r.error}</span>` : ''}
      </div>`;
    });
    wrap.innerHTML = html;
  } catch (e) {
    wrap.innerHTML = `<div class="empty"><div class="empty-text" style="color:var(--danger)">Erreur : ${e.message}</div></div>`;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// RÉSULTATS
// ─────────────────────────────────────────────────────────────────────────────

async function loadResults() {
  const results = await fetch(`${API}/results`).then(r => r.json()).catch(() => []);
  const el      = document.getElementById('results-list');
  if (!results.length) {
    el.innerHTML = '<div class="empty"><div class="empty-icon">📋</div><div class="empty-text">Aucun résultat</div></div>';
    return;
  }
  el.innerHTML = results.map((res, i) => {
    const rate  = res.summary.success_rate;
    const color = rate === 100 ? 'var(--success)' : rate >= 80 ? 'var(--warn)' : 'var(--danger)';
    const date  = new Date(res.run_time).toLocaleString('fr-FR');
    return `
      <div class="result-block">
        <div class="result-header" onclick="toggleResult(${i})">
          <span class="badge ${rate === 100 ? 'badge-success' : 'badge-danger'}" style="font-size:13px;">${rate}%</span>
          <span style="font-weight:600;">${res.suite_name}</span>
          <span class="badge badge-info">${res.source_type || 'local'}</span>
          <span style="font-size:12px;color:var(--muted);">${res.source_label || ''}</span>
          <span style="margin-left:auto;font-size:11px;color:var(--muted);">${date}</span>
        </div>
        <div class="result-body" id="result-body-${i}" style="display:none;">
          ${res.results.map(r => `
            <div class="result-row">
              <span class="badge ${r.success ? 'badge-success' : 'badge-danger'}">${r.success ? '✓' : '✗'}</span>
              <span class="exp-type" style="flex:1">${r.expectation_type}</span>
              <span class="exp-kwargs">${JSON.stringify(r.kwargs)}</span>
            </div>`).join('')}
        </div>
      </div>`;
  }).join('');
}

function toggleResult(i) {
  const el = document.getElementById(`result-body-${i}`);
  if (el) el.style.display = el.style.display === 'none' ? '' : 'none';
}

async function clearResults() {
  if (!confirm('Effacer tout l\'historique ?')) return;
  await fetch(`${API}/results`, { method: 'DELETE' });
  await loadResults();
  toast('Historique effacé', 'success');
}

// ─────────────────────────────────────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────────────────────────────────────

async function initSource() {
  try {
    const r = await fetch(`${API}/source`);
    if (r.ok) {
      const d = await r.json();
      currentSource = d.type || 'local';
    }
  } catch (_) {}
  _updateSourceTabs(currentSource);
  _showSourcePanel(currentSource);
  await refreshSource();
}

async function init() {
  await checkHealth();
  setInterval(checkHealth, 15000);
  await Promise.all([
    initSource(),
    loadData(),
    loadCatalogue(),
    loadSuites(),
  ]);
}

init();
