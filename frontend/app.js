const API = window.location.port === '8080' ? '/api' : 'http://localhost:5000/api';
let catalogue = {};
let suites = {};
let selectedSuite = null;

// ── NAVIGATION ──────────────────────────────────────────────────────────────
const TABS = { data:'Aperçu des données', suites:'Suites d\'expectations', validate:'Validation', results:'Résultats', docs:'Documentation API' };
function goto(tab) {
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  event.currentTarget.classList.add('active');
  document.querySelectorAll('.tab-panel').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-'+tab).classList.add('active');
  document.getElementById('page-title').textContent = TABS[tab];
  document.getElementById('topbar-actions').innerHTML = '';
  if (tab === 'results') loadResults();
  if (tab === 'validate') populateValidateSuites();
}

// ── TOAST ──────────────────────────────────────────────────────────────────
function toast(msg, type='info') {
  const t = document.getElementById('toast');
  const colors = { info: '#60a5fa', success: '#34d399', danger: '#f87171', warn: '#fbbf24' };
  t.style.borderLeftColor = colors[type] || '#60a5fa';
  t.style.borderLeftWidth = '3px';
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3500);
}

// ── API HEALTH ─────────────────────────────────────────────────────────────
async function checkHealth() {
  try {
    const r = await fetch(`${API}/data/preview`, {signal: AbortSignal.timeout(5000)});
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

// ── FILE SELECTOR ──────────────────────────────────────────────────────────
async function loadFiles() {
  try {
    const r = await fetch(`${API}/data/files`).then(r => r.json());
    const files = r.files || [];
    const sel = document.getElementById('file-select');
    if (!files.length) {
      sel.innerHTML = '<option value="">— Aucun fichier dans /data —</option>';
      return;
    }
    sel.innerHTML = files.map(f =>
      `<option value="${f.name}" ${f.active ? 'selected' : ''}>${f.name} (${f.size_kb} KB)</option>`
    ).join('');
    updateFileMeta(files.find(f => f.active) || files[0]);
  } catch(e) {
    document.getElementById('file-select').innerHTML = '<option>Erreur de chargement</option>';
  }
}

function updateFileMeta(file) {
  if (!file) return;
  const meta = document.getElementById('file-meta');
  meta.style.display = 'block';
  meta.innerHTML = `
    <span class="badge badge-info" style="margin-right:6px;">${file.ext.toUpperCase()}</span>
    ${file.path} &nbsp;·&nbsp; ${file.size_kb} KB
  `;
}

async function selectFile() {
  const name = document.getElementById('file-select').value;
  if (!name) return;
  try {
    await fetch(`${API}/data/select`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name})
    });
    toast(`Fichier actif : ${name}`, 'success');
    await refreshColumns();
    renderExpParams();
    await loadData();
  } catch(e) { toast('Erreur : ' + e.message, 'danger'); }
}

// ── DATA TAB ───────────────────────────────────────────────────────────────
async function loadData() {
  try {
    const [prev, stats] = await Promise.all([
      fetch(`${API}/data/preview`).then(r=>r.json()),
      fetch(`${API}/data/stats`).then(r=>r.json()),
    ]);

    if (prev.error) {
      document.getElementById('preview-table-wrap').innerHTML =
        `<div class="empty"><div class="empty-text" style="color:var(--danger)">${prev.error}</div></div>`;
      return;
    }

    document.getElementById('s-rows').textContent = prev.total_rows ?? '—';
    document.getElementById('s-cols').textContent = (prev.columns||[]).length;

    const totalNulls = Array.isArray(stats) ? stats.reduce((a,s) => a + (s.nulls||0), 0) : '—';
    document.getElementById('s-nulls').textContent = totalNulls;
    document.getElementById('s-file').textContent = (prev.file || 'CSV').split('.').pop().toUpperCase();

    // Preview table
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

    // Column profile
    if (Array.isArray(stats)) {
      let html = '';
      stats.forEach(s => {
        const nullPct = s.total ? Math.round(s.nulls / s.total * 100) : 0;
        const uniquePct = s.total ? Math.round(s.unique / s.total * 100) : 0;
        html += `<div style="margin-bottom:14px;padding-bottom:14px;border-bottom:1px solid var(--border)">
          <div class="flex-center mb-10">
            <span style="font-weight:600;font-size:13px;">${s.column}</span>
            <span class="badge badge-info" style="margin-left:8px;">${s.type}</span>
            ${s.nulls > 0 ? `<span class="badge badge-danger" style="margin-left:4px;">${s.nulls} nuls</span>` : ''}
            <span style="margin-left:auto;font-size:11px;color:var(--muted);">${s.unique} valeurs uniques (${uniquePct}%)</span>
          </div>
          <div style="display:flex;gap:8px;align-items:center;">
            <div style="font-size:11px;color:var(--muted);width:60px">Nulls ${nullPct}%</div>
            <div class="progress-bar" style="flex:1"><div class="progress-fill" style="width:${nullPct}%;background:${nullPct>20?'var(--danger)':'var(--success)'};"></div></div>
          </div>
          ${s.min !== undefined ? `<div style="margin-top:6px;font-size:11px;font-family:var(--mono);color:var(--muted)">min: ${s.min} · max: ${s.max} · mean: ${s.mean?.toFixed(1)}</div>` : ''}
        </div>`;
      });
      document.getElementById('col-profile').innerHTML = html;
    }
  } catch(e) {
    document.getElementById('preview-table-wrap').innerHTML = `<div class="empty"><div class="empty-text" style="color:var(--danger)">Erreur de chargement : ${e.message}</div></div>`;
    document.getElementById('col-profile').innerHTML = '';
  }
}

// ── CATALOGUE ─────────────────────────────────────────────────────────────
let availableColumns = [];

async function loadCatalogue() {
  try {
    catalogue = await fetch(`${API}/expectations/catalogue`).then(r=>r.json());
    const sel = document.getElementById('exp-type');
    sel.innerHTML = Object.entries(catalogue).map(([k,v]) =>
      `<option value="${k}">${v.label} — ${k}</option>`
    ).join('');
    await refreshColumns();
    renderExpParams();
  } catch {}
}

async function refreshColumns() {
  try {
    const r = await fetch(`${API}/data/columns`).then(r=>r.json());
    availableColumns = r.columns || [];
  } catch { availableColumns = []; }
}

function renderExpParams() {
  const type = document.getElementById('exp-type').value;
  const info = catalogue[type];
  if (!info) return;
  const params = info.params || [];
  let html = '';
  params.forEach(p => {
    if (p === 'column') {
      // Dropdown peuplé avec les colonnes réelles du fichier actif
      const opts = availableColumns.length
        ? availableColumns.map(c => `<option value="${c.name}">${c.name} (${c.type})</option>`).join('')
        : `<option value="">— Chargement des colonnes… —</option>`;
      html += `<div class="form-row"><div class="form-col">
        <div class="form-label">Colonne <span style="color:var(--accent);font-size:10px;">(fichier actif)</span></div>
        <select class="input" id="param-column">${opts}</select>
      </div></div>`;
    } else {
      html += `<div class="form-row"><div class="form-col">
        <div class="form-label">${p}</div>
        <input class="input" id="param-${p}" placeholder="${p}">
      </div></div>`;
    }
  });
  if (info.description) {
    html += `<div style="font-size:11px;color:var(--muted);margin-bottom:8px;padding:8px 10px;background:var(--bg);border-radius:6px;border:1px solid var(--border)">${info.description}</div>`;
  }
  document.getElementById('exp-params').innerHTML = html;
}

// ── SUITES ─────────────────────────────────────────────────────────────────
async function loadSuites() {
  suites = await fetch(`${API}/suites`).then(r=>r.json()).catch(()=>({}));
  renderSuiteList();
}

function renderSuiteList() {
  const names = Object.keys(suites);
  if (!names.length) {
    document.getElementById('suite-list').innerHTML = '<div class="empty"><div class="empty-icon">📋</div><div class="empty-text">Aucune suite créée</div></div>';
    return;
  }
  document.getElementById('suite-list').innerHTML = names.map(n => {
    const s = suites[n];
    const count = (s.expectations||[]).length;
    return `<div class="suite-item ${selectedSuite===n?'selected':''}" onclick="selectSuite('${n}')">
      <div>
        <div class="suite-name">${n}</div>
        <div class="suite-meta">${count} expectation${count!==1?'s':''}</div>
      </div>
      <span class="badge badge-info">${count}</span>
    </div>`;
  }).join('');
}

function selectSuite(name) {
  selectedSuite = name;
  document.getElementById('suite-editor').style.display = 'block';
  document.getElementById('suite-placeholder').style.display = 'none';
  document.getElementById('editor-title').textContent = name;
  renderExpList();
  renderSuiteList();
}

function renderExpList() {
  const s = suites[selectedSuite];
  const exps = s?.expectations || [];
  if (!exps.length) {
    document.getElementById('exp-list').innerHTML = '<div style="font-size:12px;color:var(--muted);margin-bottom:8px;">Aucune expectation. Ajoutez-en une ci-dessous.</div>';
    return;
  }
  document.getElementById('exp-list').innerHTML = exps.map((e,i) => {
    const kw = Object.entries(e.kwargs||{}).map(([k,v])=>`${k}=${JSON.stringify(v)}`).join(', ');
    return `<div class="exp-item">
      <div>
        <div class="exp-type">${e.expectation_type}</div>
        <div class="exp-kwargs">${kw}</div>
      </div>
      <button class="btn btn-danger btn-sm ml-auto" onclick="deleteExpectation(${i})">✕</button>
    </div>`;
  }).join('');
}

async function addExpectation() {
  if (!selectedSuite) return;
  const type = document.getElementById('exp-type').value;
  const info = catalogue[type];
  const kwargs = {};
  (info?.params||[]).forEach(p => {
    const el = document.getElementById(`param-${p}`);
    if (el?.value) kwargs[p] = el.value;
  });
  try {
    await fetch(`${API}/suites/${selectedSuite}/expectations`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ expectation_type: type, kwargs })
    }).then(r=>r.json());
    await loadSuites();
    selectSuite(selectedSuite);
    toast('Expectation ajoutée !', 'success');
  } catch(e) { toast('Erreur: ' + e.message, 'danger'); }
}

async function deleteExpectation(idx) {
  try {
    await fetch(`${API}/suites/${selectedSuite}/expectations/${idx}`, {method:'DELETE'});
    await loadSuites();
    selectSuite(selectedSuite);
    toast('Supprimée', 'info');
  } catch(e) { toast('Erreur', 'danger'); }
}

function showCreateSuite() {
  const m = document.getElementById('create-suite-modal');
  m.style.display = 'flex';
}
function hideModal() {
  document.getElementById('create-suite-modal').style.display = 'none';
}
async function createSuite() {
  const name = document.getElementById('new-suite-name').value.trim();
  if (!name) return;
  try {
    const r = await fetch(`${API}/suites`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({name})
    }).then(r=>r.json());
    hideModal();
    document.getElementById('new-suite-name').value = '';
    await loadSuites();
    selectSuite(name);
    toast(`Suite "${name}" créée !`, 'success');
  } catch(e) { toast('Erreur : ' + e.message, 'danger'); }
}

async function deleteSuite() {
  if (!selectedSuite) return;
  if (!confirm(`Supprimer la suite "${selectedSuite}" ?`)) return;
  await fetch(`${API}/suites/${selectedSuite}`, {method:'DELETE'});
  selectedSuite = null;
  document.getElementById('suite-editor').style.display = 'none';
  document.getElementById('suite-placeholder').style.display = 'flex';
  await loadSuites();
  toast('Suite supprimée.', 'warn');
}

// ── VALIDATION ─────────────────────────────────────────────────────────────
function populateValidateSuites() {
  const sel = document.getElementById('val-suite-select');
  const names = Object.keys(suites);
  if (!names.length) {
    sel.innerHTML = '<option value="">— Aucune suite disponible —</option>';
    return;
  }
  sel.innerHTML = names.map(n => `<option value="${n}">${n}</option>`).join('');
}

async function runValidation() {
  const suite = document.getElementById('val-suite-select').value;
  if (!suite) { toast('Sélectionnez une suite', 'warn'); return; }
  document.getElementById('val-running').style.display = 'flex';
  document.getElementById('val-result-container').innerHTML = '';
  try {
    const res = await fetch(`${API}/validate/${suite}`, {method:'POST'}).then(r=>r.json());
    document.getElementById('val-running').style.display = 'none';
    if (res.error) { toast('Erreur : ' + res.error, 'danger'); return; }
    renderValidationResult(res, 'val-result-container');
    toast('Validation terminée !', 'success');
  } catch(e) {
    document.getElementById('val-running').style.display = 'none';
    toast('Erreur réseau: ' + e.message, 'danger');
  }
}

function renderValidationResult(res, container) {
  const s = res.summary;
  const pct = s.success_rate;
  const color = pct >= 80 ? 'var(--success)' : pct >= 50 ? 'var(--warn)' : 'var(--danger)';

  let warningsHtml = '';
  if (res.warnings && res.warnings.length) {
    warningsHtml = `<div style="margin-bottom:14px;padding:10px 14px;background:rgba(251,191,36,0.08);border:1px solid rgba(251,191,36,0.3);border-radius:8px;">
      <div style="font-size:11px;font-weight:600;color:var(--warn);margin-bottom:6px;">⚠ Colonnes non trouvées dans le fichier actif</div>
      ${res.warnings.map(w => `<div style="font-size:11px;font-family:var(--mono);color:var(--muted);margin-top:3px;">${w.message}</div>`).join('')}
    </div>`;
  }

  let html = `
    <div class="grid-4 mb-16" style="margin-top:16px;">
      <div class="stat-card"><div class="stat-val" style="color:${color}">${pct}%</div><div class="stat-label">Taux de succès</div></div>
      <div class="stat-card"><div class="stat-val stat-accent">${s.total}</div><div class="stat-label">Tests total</div></div>
      <div class="stat-card"><div class="stat-val stat-success">${s.passed}</div><div class="stat-label">Réussis</div></div>
      <div class="stat-card"><div class="stat-val stat-danger">${s.failed}</div><div class="stat-label">Échoués</div></div>
    </div>
    <div class="card">
      <div class="flex-center mb-16">
        <div class="card-title" style="margin:0">Détail — ${res.suite_name}</div>
        <span style="margin-left:8px;font-size:11px;color:var(--muted);font-family:var(--mono);">sur ${res.source_file}</span>
        <span style="margin-left:auto;font-size:11px;color:var(--muted);font-family:var(--mono);">${res.run_time}</span>
      </div>
      ${warningsHtml}
      <div class="progress-bar mb-16"><div class="progress-fill" style="width:${pct}%;background:${color};"></div></div>
  `;
  res.results.forEach(r => {
    const kw = Object.entries(r.kwargs||{}).map(([k,v])=>`${k}=${JSON.stringify(v)}`).join(', ');
    html += `<div class="result-row">
      <span class="badge ${r.success?'badge-success':'badge-danger'}">${r.success?'✓ PASS':'✗ FAIL'}</span>
      <span class="exp-type" style="flex:1">${r.expectation_type}</span>
      <span class="exp-kwargs">${kw}</span>
      ${r.error ? `<span style="font-size:10px;color:var(--danger);margin-left:8px;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${r.error}">⚠ ${r.error.split('\n')[0]}</span>` : ''}
    </div>`;
  });
  html += '</div>';
  document.getElementById(container).innerHTML = html;
}

// ── RESULTS ────────────────────────────────────────────────────────────────
async function loadResults() {
  const results = await fetch(`${API}/results`).then(r=>r.json()).catch(()=>[]);
  document.getElementById('results-count').textContent = `${results.length} run(s) en historique`;
  if (!results.length) {
    document.getElementById('results-list').innerHTML = '<div class="empty"><div class="empty-icon">📊</div><div class="empty-text">Aucun résultat. Lancez une validation d\'abord.</div></div>';
    return;
  }
  document.getElementById('results-list').innerHTML = results.map((res,i) => {
    const s = res.summary;
    const pct = s.success_rate;
    const color = pct >= 80 ? 'var(--success)' : pct >= 50 ? 'var(--warn)' : 'var(--danger)';
    return `<div class="result-block">
      <div class="result-header" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display==='block'?'none':'block'">
        <span class="badge ${pct>=80?'badge-success':pct>=50?'badge-warn':'badge-danger'}">${pct}%</span>
        <span style="font-weight:600;font-size:14px;">${res.suite_name}</span>
        <span class="text-muted text-sm text-mono">${s.passed}/${s.total} tests</span>
        <span class="text-muted text-sm text-mono ml-auto">${res.run_time}</span>
      </div>
      <div class="result-body" style="display:none">
        ${res.results.map(r => {
          const kw = Object.entries(r.kwargs||{}).map(([k,v])=>`${k}=${JSON.stringify(v)}`).join(', ');
          return `<div class="result-row">
            <span class="badge ${r.success?'badge-success':'badge-danger'}">${r.success?'✓':'✗'}</span>
            <span class="exp-type" style="flex:1">${r.expectation_type}</span>
            <span class="exp-kwargs">${kw}</span>
          </div>`;
        }).join('')}
      </div>
    </div>`;
  }).join('');
}

async function clearResults() {
  if (!confirm('Effacer tout l\'historique ?')) return;
  await fetch(`${API}/results`, {method:'DELETE'});
  loadResults();
  toast('Historique effacé.', 'warn');
}

// ── INIT ───────────────────────────────────────────────────────────────────
async function init() {
  await checkHealth();
  setInterval(checkHealth, 15000);
  await Promise.all([loadFiles(), loadData(), loadCatalogue(), loadSuites()]);
}
init();