// ─────────────────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────────────────

const API = window.location.port === '8080' ? '/api' : 'http://localhost:5000/api';

const TAB_TITLES = {
  data:     'Aperçu des données',
  suites:   'Suites d\'expectations',
  validate: 'Validation',
  results:  'Résultats',
  catalog:  'Catalogue des sources',
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
    // Stocker {name, type} pour chaque colonne
    availableColumns = (data.columns || []).map(c =>
      typeof c === 'object' ? c : { name: c, type: '' }
    );
  } catch {}
}

function renderExpParams() {
  const type = document.getElementById('exp-type')?.value;
  const wrap = document.getElementById('exp-params-wrap');
  if (!wrap || !type || !catalogue[type]) return;

  const params = catalogue[type].params || [];
  wrap.innerHTML = params.map(p => {
    if (p === 'column') {
      // Afficher nom(type) dans la dropdown — valeur = nom seul
      const opts = availableColumns.map(c => {
        const label = c.type ? `${c.name} (${c.type})` : c.name;
        return `<option value="${c.name}">${label}</option>`;
      }).join('');
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
    loadCatalog(),
  ]);
}

init();

// ─────────────────────────────────────────────────────────────────────────────
// CATALOGUE DES SOURCES
// ─────────────────────────────────────────────────────────────────────────────

let catalogData = {};  // toutes les entrées

const SOURCE_TYPE_LABELS = { local: '📁 Local', minio: '🪣 MinIO', postgres: '🐘 PostgreSQL', '': '—' };
const TAG_COLORS = [
  'rgba(108,110,255,.15)', 'rgba(157,110,255,.15)', 'rgba(52,211,153,.12)',
  'rgba(251,191,36,.12)',  'rgba(96,165,250,.12)',   'rgba(248,113,113,.12)',
];
function _tagColor(tag) {
  let h = 0;
  for (const c of tag) h = (h * 31 + c.charCodeAt(0)) & 0xffff;
  return TAG_COLORS[h % TAG_COLORS.length];
}
function _tagHtml(tag) {
  return `<span style="display:inline-block;padding:2px 8px;border-radius:20px;font-size:10px;font-weight:600;font-family:var(--mono);background:${_tagColor(tag)};color:var(--text);margin:2px 2px 0 0;">${tag}</span>`;
}

async function loadCatalog() {
  try {
    const r = await fetch(`${API}/catalog`);
    if (!r.ok) return;
    catalogData = await r.json();
    renderCatalog();
  } catch {}
}

function renderCatalog(entries = null) {
  const grid = document.getElementById('catalog-grid');
  if (!grid) return;
  const data = entries || Object.values(catalogData);
  if (!data.length) {
    grid.innerHTML = '<div class="empty" style="grid-column:1/-1"><div class="empty-icon">🗂️</div><div class="empty-text">Aucune source décrite — cliquez sur "+ Nouvelle entrée"</div></div>';
    return;
  }
  grid.innerHTML = data.map(e => `
    <div class="card" style="margin:0;position:relative;">
      <div class="flex-center" style="margin-bottom:8px;gap:8px;">
        <span style="font-weight:700;font-size:14px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${e.name}">${e.alias || e.name}</span>
        ${e.source_type ? `<span class="badge badge-info" style="font-size:10px;white-space:nowrap;">${SOURCE_TYPE_LABELS[e.source_type] || e.source_type}</span>` : ''}
      </div>
      ${e.name !== (e.alias || e.name) ? `<div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-bottom:6px;">${e.name}</div>` : ''}
      ${e.description ? `<div style="font-size:12px;color:var(--muted);margin-bottom:8px;line-height:1.5;">${e.description}</div>` : ''}
      ${e.source_ref  ? `<div style="font-size:11px;font-family:var(--mono);color:var(--accent);margin-bottom:6px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${e.source_ref}">⟶ ${e.source_ref}</div>` : ''}
      ${e.owner ? `<div style="font-size:11px;color:var(--muted);margin-bottom:6px;">👤 ${e.owner}</div>` : ''}
      ${e.tags?.length ? `<div style="margin-bottom:10px;">${e.tags.map(_tagHtml).join('')}</div>` : ''}
      ${e.fields?.length ? `
        <details style="margin-bottom:10px;">
          <summary style="font-size:11px;color:var(--muted);cursor:pointer;user-select:none;font-weight:600;letter-spacing:.05em;text-transform:uppercase;">
            ${e.fields.length} champ${e.fields.length > 1 ? 's' : ''}
          </summary>
          <div style="margin-top:8px;border:1px solid var(--border);border-radius:6px;overflow:hidden;">
            <table style="width:100%;border-collapse:collapse;font-size:11px;">
              <thead><tr style="background:var(--bg3);">
                <th style="padding:5px 8px;text-align:left;color:var(--muted);font-weight:600;">Champ</th>
                <th style="padding:5px 8px;text-align:left;color:var(--muted);font-weight:600;">Type</th>
                <th style="padding:5px 8px;text-align:left;color:var(--muted);font-weight:600;">Description</th>
                <th style="padding:5px 8px;text-align:center;color:var(--muted);font-weight:600;">Nullable</th>
              </tr></thead>
              <tbody>${e.fields.map(f => `
                <tr style="border-top:1px solid var(--border);">
                  <td style="padding:5px 8px;font-family:var(--mono);color:var(--accent);">${f.name}</td>
                  <td style="padding:5px 8px;font-family:var(--mono);color:var(--muted);">${f.type || '—'}</td>
                  <td style="padding:5px 8px;color:var(--text);">${f.description || ''}</td>
                  <td style="padding:5px 8px;text-align:center;">${f.nullable !== false ? '✓' : '✗'}</td>
                </tr>`).join('')}
              </tbody>
            </table>
          </div>
        </details>` : ''}
      <div style="display:flex;gap:6px;margin-top:auto;padding-top:10px;border-top:1px solid var(--border);">
        <button class="btn btn-ghost btn-sm" onclick="openCatalogModal('${e.name}')" style="flex:1;">✏️ Modifier</button>
        <button class="btn btn-danger btn-sm" onclick="deleteCatalogEntry('${e.name}')">✕</button>
      </div>
    </div>`
  ).join('');
}

function filterCatalog() {
  const search = (document.getElementById('catalog-search')?.value || '').toLowerCase();
  const tag    = (document.getElementById('catalog-tag-filter')?.value || '').toLowerCase().trim();
  let entries  = Object.values(catalogData);
  if (search) {
    entries = entries.filter(e =>
      e.name.toLowerCase().includes(search) ||
      (e.alias || '').toLowerCase().includes(search) ||
      (e.owner || '').toLowerCase().includes(search) ||
      (e.description || '').toLowerCase().includes(search)
    );
  }
  if (tag) {
    entries = entries.filter(e => (e.tags || []).some(t => t.toLowerCase().includes(tag)));
  }
  renderCatalog(entries);
}

// ── Modal ──────────────────────────────────────────────────────────────────────

// ── Sélecteur de référence dynamique dans le modal catalog ───────────────────

async function onCatalogSourceTypeChange() {
  const type = document.getElementById('cf-source-type').value;
  // Cacher tous les sélecteurs spécifiques
  ['cf-minio-wrap','cf-pg-wrap','cf-local-wrap'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = 'none';
  });
  // Remettre le champ texte visible par défaut
  const refWrap = document.getElementById('cf-source-ref-wrap');
  if (refWrap) refWrap.style.display = '';
  document.getElementById('cf-source-ref').value = '';
  _renderFieldsEditor([]);

  if (type === 'local') {
    refWrap.style.display = 'none';
    const wrap = document.getElementById('cf-local-wrap');
    if (wrap) wrap.style.display = '';
    await _loadCatalogLocalFiles();
  } else if (type === 'minio') {
    refWrap.style.display = 'none';
    const wrap = document.getElementById('cf-minio-wrap');
    if (wrap) wrap.style.display = '';
    await _loadCatalogMinioBuckets();
  } else if (type === 'postgres') {
    refWrap.style.display = 'none';
    const wrap = document.getElementById('cf-pg-wrap');
    if (wrap) wrap.style.display = '';
    await _loadCatalogPgTables();
  }
}

async function _loadCatalogLocalFiles() {
  const sel = document.getElementById('cf-local-file');
  if (!sel) return;
  try {
    const r = await fetch(`${API}/data/files`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data  = await r.json();
    const files = data.files || [];
    sel.innerHTML = '<option value="">— choisir un fichier —</option>' +
      files.map(f => `<option value="${f.name}">${f.name} (${f.size_kb} KB)</option>`).join('');
  } catch (e) {
    sel.innerHTML = `<option value="">Erreur : ${e.message}</option>`;
  }
}

async function _loadCatalogMinioBuckets() {
  const sel = document.getElementById('cf-minio-bucket');
  if (!sel) return;
  try {
    const r = await fetch(`${API}/minio/buckets`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    sel.innerHTML = '<option value="">— choisir un bucket —</option>' +
      (data.buckets || []).map(b => `<option value="${b}">${b}</option>`).join('');
    document.getElementById('cf-minio-file').innerHTML = '<option value="">— choisir un bucket —</option>';
  } catch (e) {
    sel.innerHTML = `<option value="">Erreur : ${e.message}</option>`;
  }
}

async function onCatalogMinioBucketChange() {
  const bucket = document.getElementById('cf-minio-bucket')?.value;
  const sel    = document.getElementById('cf-minio-file');
  if (!bucket || !sel) return;
  try {
    const r = await fetch(`${API}/minio/files?bucket=${encodeURIComponent(bucket)}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data  = await r.json();
    const files = data.files || [];
    sel.innerHTML = '<option value="">— choisir un fichier —</option>' +
      files.map(f => `<option value="${f.name}">${f.name} (${f.size_kb} KB)</option>`).join('');
  } catch (e) {
    sel.innerHTML = `<option value="">Erreur : ${e.message}</option>`;
  }
}

async function _loadCatalogPgTables() {
  const sel = document.getElementById('cf-pg-table');
  if (!sel) return;
  try {
    const r = await fetch(`${API}/postgres/tables`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    sel.innerHTML = '<option value="">— choisir une table —</option>' +
      (data.tables || []).map(t => `<option value="${t}">${t}</option>`).join('');
  } catch (e) {
    sel.innerHTML = `<option value="">Erreur : ${e.message}</option>`;
  }
}

// Appelé quand l'user sélectionne une ref → charger les colonnes depuis l'API
async function onCatalogRefSelected(type) {
  let ref = '';
  if (type === 'local') {
    ref = document.getElementById('cf-local-file')?.value;
  } else if (type === 'minio') {
    const bucket = document.getElementById('cf-minio-bucket')?.value;
    const file   = document.getElementById('cf-minio-file')?.value;
    if (bucket && file) ref = `${bucket}/${file}`;
  } else if (type === 'postgres') {
    ref = document.getElementById('cf-pg-table')?.value;
  }
  if (!ref) return;

  // Mettre à jour source_ref caché (pour la sauvegarde)
  document.getElementById('cf-source-ref').value = ref;

  // Charger les colonnes via l'API correspondante pour pré-remplir le tableau
  await _fetchAndFillFields(type, ref);
}

async function _fetchAndFillFields(type, ref) {
  if (!ref) return;
  _renderFieldsEditor([{ name: '…', type: 'chargement', description: '', nullable: true }]);
  try {
    let columns = [];
    if (type === 'local') {
      // Sélectionner temporairement le fichier pour récupérer ses colonnes
      await fetch(`${API}/data/select`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: ref }),
      });
      const r = await fetch(`${API}/data/columns`);
      if (r.ok) { const d = await r.json(); columns = d.columns || []; }
    } else if (type === 'minio') {
      const [bucket, ...keyParts] = ref.split('/');
      const key = keyParts.join('/');
      await fetch(`${API}/minio/select`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ bucket, key }),
      });
      const r = await fetch(`${API}/data/columns`);
      if (r.ok) { const d = await r.json(); columns = d.columns || []; }
    } else if (type === 'postgres') {
      await fetch(`${API}/postgres/select`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table: ref }),
      });
      const r = await fetch(`${API}/data/columns`);
      if (r.ok) { const d = await r.json(); columns = d.columns || []; }
    }

    // columns = [{name, type}] — pré-remplir le tableau avec type déjà renseigné
    _renderFieldsEditor(columns.map(c => ({
      name:        typeof c === 'object' ? c.name : c,
      type:        typeof c === 'object' ? (c.type || '') : '',
      description: '',
      nullable:    true,
    })));
  } catch (e) {
    _renderFieldsEditor([]);
    console.warn('Impossible de charger les colonnes :', e.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────

function openCatalogModal(key = null) {
  const overlay = document.getElementById('catalog-modal-overlay');
  const errEl   = document.getElementById('catalog-modal-error');
  if (errEl) errEl.style.display = 'none';

  // Reset tous les champs
  ['cf-name','cf-alias','cf-description','cf-owner','cf-tags','cf-source-ref'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
  document.getElementById('cf-source-type').value = '';
  document.getElementById('catalog-edit-key').value = '';

  // Cacher les sélecteurs spécifiques, montrer le champ texte générique
  ['cf-minio-wrap','cf-pg-wrap','cf-local-wrap'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = 'none';
  });
  const refWrap = document.getElementById('cf-source-ref-wrap');
  if (refWrap) refWrap.style.display = '';
  _renderFieldsEditor([]);

  if (key && catalogData[key]) {
    // ── Mode édition ──
    const e = catalogData[key];
    document.getElementById('catalog-modal-title').textContent = 'Modifier la source';
    document.getElementById('catalog-edit-key').value   = key;
    document.getElementById('cf-name').value            = e.name;
    document.getElementById('cf-name').disabled         = true;
    document.getElementById('cf-alias').value           = e.alias || '';
    document.getElementById('cf-description').value     = e.description || '';
    document.getElementById('cf-owner').value           = e.owner || '';
    document.getElementById('cf-tags').value            = (e.tags || []).join(', ');
    document.getElementById('cf-source-type').value     = e.source_type || '';
    document.getElementById('cf-source-ref').value      = e.source_ref || '';
    // Afficher les champs sauvegardés (types déjà connus, modifiables)
    _renderFieldsEditor(e.fields || []);
    // Si type connu → afficher le bon sélecteur en mode lecture seule info
    if (e.source_type) _showCatalogRefInfo(e.source_type, e.source_ref);
  } else {
    // ── Mode création ──
    document.getElementById('catalog-modal-title').textContent = 'Nouvelle source';
    document.getElementById('cf-name').disabled = false;
    // Si une source est déjà active dans l'app → pré-remplir
    if (currentSource && currentSource !== 'local' || availableColumns.length) {
      document.getElementById('cf-source-type').value = currentSource || '';
      _renderFieldsEditor(availableColumns.map(c => ({
        name: c.name || c, type: c.type || '', description: '', nullable: true,
      })));
    }
  }

  overlay.style.display = 'flex';
}

// Afficher l'info de référence en mode édition (lecture seule, pas de rechargement)
function _showCatalogRefInfo(type, ref) {
  if (!ref) return;
  const refWrap = document.getElementById('cf-source-ref-wrap');
  if (refWrap) refWrap.style.display = '';
  // Afficher le champ texte avec la ref actuelle en readonly visuel
  const input = document.getElementById('cf-source-ref');
  if (input) { input.value = ref; input.style.color = 'var(--muted)'; }
}

// ── Éditeur de champs (tableau inline dans le modal) ─────────────────────────

function _renderFieldsEditor(fields) {
  const wrap = document.getElementById('cf-fields-wrap');
  if (!wrap) return;

  const rows = fields.map((f, i) => `
    <tr id="cf-field-row-${i}">
      <td style="padding:4px 6px;"><input class="input" style="font-size:11px;font-family:var(--mono);padding:5px 8px;" id="cff-name-${i}" value="${f.name || ''}" placeholder="nom_champ"></td>
      <td style="padding:4px 6px;"><input class="input" style="font-size:11px;font-family:var(--mono);padding:5px 8px;" id="cff-type-${i}" value="${f.type || ''}" placeholder="string"></td>
      <td style="padding:4px 6px;"><input class="input" style="font-size:11px;padding:5px 8px;" id="cff-desc-${i}" value="${f.description || ''}" placeholder="Description du champ…"></td>
      <td style="padding:4px 6px;text-align:center;">
        <input type="checkbox" id="cff-null-${i}" ${f.nullable !== false ? 'checked' : ''} style="width:16px;height:16px;cursor:pointer;accent-color:var(--accent);">
      </td>
      <td style="padding:4px 6px;">
        <button class="btn btn-danger btn-sm" style="padding:3px 7px;" onclick="_removeFieldRow(${i})">✕</button>
      </td>
    </tr>`).join('');

  wrap.innerHTML = `
    <div style="margin-bottom:6px;display:flex;align-items:center;justify-content:space-between;">
      <span class="form-label" style="margin:0;">Champs / colonnes</span>
      <button class="btn btn-ghost btn-sm" onclick="_addFieldRow()" style="font-size:11px;">+ Ajouter un champ</button>
    </div>
    ${fields.length ? `
    <div style="overflow-x:auto;border:1px solid var(--border);border-radius:7px;">
      <table style="width:100%;border-collapse:collapse;font-size:12px;" id="cf-fields-table">
        <thead>
          <tr style="background:var(--bg3);">
            <th style="padding:6px 8px;text-align:left;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.06em;min-width:120px;">Nom</th>
            <th style="padding:6px 8px;text-align:left;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.06em;min-width:90px;">Type</th>
            <th style="padding:6px 8px;text-align:left;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.06em;">Description</th>
            <th style="padding:6px 8px;text-align:center;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.06em;width:60px;">Nullable</th>
            <th style="width:36px;"></th>
          </tr>
        </thead>
        <tbody id="cf-fields-tbody">${rows}</tbody>
      </table>
    </div>` : `<div id="cf-fields-tbody"></div>`}`;
}

function _addFieldRow() {
  // Lire les lignes existantes, ajouter une vide
  const existing = _collectFields();
  _renderFieldsEditor([...existing, { name: '', type: '', description: '', nullable: true }]);
}

function _removeFieldRow(i) {
  const fields = _collectFields();
  fields.splice(i, 1);
  _renderFieldsEditor(fields);
}

function _collectFields() {
  const tbody = document.getElementById('cf-fields-tbody');
  if (!tbody) return [];
  const rows = tbody.querySelectorAll('tr[id^="cf-field-row-"]');
  return Array.from(rows).map((_, i) => ({
    name:        document.getElementById(`cff-name-${i}`)?.value.trim() || '',
    type:        document.getElementById(`cff-type-${i}`)?.value.trim() || '',
    description: document.getElementById(`cff-desc-${i}`)?.value.trim() || '',
    nullable:    document.getElementById(`cff-null-${i}`)?.checked ?? true,
  })).filter(f => f.name);  // ignorer les lignes sans nom
}

// ─────────────────────────────────────────────────────────────────────────────

function closeCatalogModal() {
  document.getElementById('catalog-modal-overlay').style.display = 'none';
  document.getElementById('cf-name').disabled = false;
}

async function saveCatalogEntry() {
  const errEl   = document.getElementById('catalog-modal-error');
  const editKey = document.getElementById('catalog-edit-key').value;
  const isEdit  = !!editKey;
  const srcType = document.getElementById('cf-source-type').value;

  // Résoudre source_ref selon le type actif
  let sourceRef = document.getElementById('cf-source-ref').value.trim();
  if (srcType === 'local' && document.getElementById('cf-local-file')?.value) {
    sourceRef = document.getElementById('cf-local-file').value;
  } else if (srcType === 'minio') {
    const bucket = document.getElementById('cf-minio-bucket')?.value;
    const file   = document.getElementById('cf-minio-file')?.value;
    if (bucket && file) sourceRef = `${bucket}/${file}`;
  } else if (srcType === 'postgres' && document.getElementById('cf-pg-table')?.value) {
    sourceRef = document.getElementById('cf-pg-table').value;
  }

  const payload = {
    name:        document.getElementById('cf-name').value.trim(),
    alias:       document.getElementById('cf-alias').value.trim(),
    description: document.getElementById('cf-description').value.trim(),
    owner:       document.getElementById('cf-owner').value.trim(),
    tags:        document.getElementById('cf-tags').value.split(',').map(t => t.trim()).filter(Boolean),
    source_type: srcType,
    source_ref:  sourceRef,
    fields:      _collectFields(),
  };

  if (!payload.name) {
    errEl.textContent = 'Le nom est obligatoire.';
    errEl.style.display = 'block';
    return;
  }

  const url    = isEdit ? `${API}/catalog/${editKey}` : `${API}/catalog`;
  const method = isEdit ? 'PUT' : 'POST';

  try {
    const r = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const d = await r.json();
    if (!r.ok) {
      errEl.textContent = d.error || `Erreur ${r.status}`;
      errEl.style.display = 'block';
      return;
    }
    closeCatalogModal();
    await loadCatalog();
    toast(isEdit ? 'Entrée mise à jour' : 'Entrée créée', 'success');
  } catch (e) {
    errEl.textContent = e.message;
    errEl.style.display = 'block';
  }
}

async function deleteCatalogEntry(key) {
  if (!confirm(`Supprimer "${key}" du catalogue ?`)) return;
  const r = await fetch(`${API}/catalog/${key}`, { method: 'DELETE' });
  if (!r.ok) { toast('Erreur suppression', 'danger'); return; }
  await loadCatalog();
  toast('Entrée supprimée', 'success');
}

