from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/ui", include_in_schema=False)
def serve_ui() -> HTMLResponse:
    """Serve a simple single-page UI for metadata browsing."""
    return HTMLResponse(CONTENT)


# Keeping HTML inline to avoid extra static asset plumbing.
CONTENT = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CHT Web Interface</title>
  <style>
    :root {
      --primary: #3b82f6;
      --primary-light: #1e40af;
      --secondary: #64748b;
      --success: #10b981;
      --danger: #ef4444;
      --warning: #f59e0b;
      --text: #f1f5f9;
      --text-muted: #94a3b8;
      --bg-main: #0f172a;
      --bg-card: #1e293b;
      --border: #334155;
      --hover: #475569;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font: 14px system-ui, sans-serif; color: var(--text); background: var(--bg-main); }
    .app-layout { display: flex; height: 100vh; }
    .sidebar { width: 320px; background: var(--bg-card); border-right: 1px solid var(--border); padding: 16px; overflow-y: auto; }
    .main-content { flex: 1; padding: 16px; overflow-y: auto; }
    .card { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 16px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
    .card-compact { padding: 12px; }
    .row { display: flex; gap: 12px; align-items: center; margin-bottom: 8px; }
    .row > * { flex: 1; }
    h1 { color: var(--text); margin-bottom: 16px; padding: 0 16px; }
    h2 { color: var(--text); margin-bottom: 12px; font-size: 16px; }
    h3 { color: var(--text); font-size: 14px; margin-bottom: 8px; }
    label { display: block; margin-bottom: 4px; color: var(--text-muted); font-weight: 500; font-size: 13px; }
    input, select, textarea { width: 100%; padding: 8px 12px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; background: var(--bg-main); color: var(--text); }
    input:focus, select:focus, textarea:focus { outline: none; border-color: var(--primary); box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2); }
    button { background: var(--primary); color: white; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-weight: 500; font-size: 13px; transition: all 0.2s ease; }
    button:hover { background: var(--primary-light); transform: translateY(-1px); }
    button:disabled { background: var(--hover); cursor: not-allowed; opacity: 0.6; }
    .btn-secondary { background: var(--hover); color: var(--text); border: 1px solid var(--border); }
    .btn-secondary:hover { background: var(--border); }
    .btn-small { padding: 6px 12px; font-size: 12px; }
    .muted { color: var(--text-muted); font-size: 12px; }
    .error { color: var(--danger); background: rgba(239, 68, 68, 0.1); border: 1px solid rgba(239, 68, 68, 0.3); padding: 12px; border-radius: 6px; }
    .success { color: var(--success); }
    .info-box { background: rgba(59, 130, 246, 0.1); border: 1px solid rgba(59, 130, 246, 0.3); padding: 12px; border-radius: 6px; }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); }
    th { background: var(--hover); font-weight: 600; color: var(--text); }
    tr:hover { background: var(--hover); }
    #loader { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(15, 23, 42, 0.9); display: none; align-items: center; justify-content: center; z-index: 1000; }
    .spinner { width: 40px; height: 40px; border: 4px solid var(--border); border-left: 4px solid var(--primary); border-radius: 50%; animation: spin 1s linear infinite; }
    @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    .status { margin: 12px 0; font-weight: 500; font-size: 13px; color: var(--text-muted); }
    .collapsible-header { cursor: pointer; display: flex; align-items: center; justify-content: space-between; padding: 12px; margin: -12px; border-radius: 6px; transition: background 0.2s ease; }
    .collapsible-header:hover { background: var(--hover); }
    .collapsible-content { transition: all 0.3s ease; overflow: hidden; }
    .collapsed { max-height: 0; opacity: 0; }
    .expanded { max-height: 1000px; opacity: 1; }
    .chevron { display: inline-flex; align-items: center; justify-content: center; width: 20px; height: 20px; border-radius: 4px; background: var(--border); color: var(--text); font-size: 12px; transition: all 0.3s ease; }
    .chevron.down { transform: rotate(180deg); background: var(--primary); }
    .table-list { max-height: 300px; overflow-y: auto; border: 1px solid var(--border); border-radius: 6px; background: var(--bg-main); }
    .table-item { padding: 8px 12px; margin: 0; border-bottom: 1px solid var(--border); cursor: pointer; font-size: 13px; transition: all 0.2s ease; }
    .table-item:last-child { border-bottom: none; }
    .table-item:hover { background: var(--hover); }
    .table-item.selected { background: var(--primary); color: white; }
    /* Modal styles */
    .modal { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); z-index: 1000; display: flex; align-items: center; justify-content: center; }
    .modal-content { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; max-width: 500px; width: 90%; max-height: 80vh; overflow-y: auto; box-shadow: 0 8px 16px rgba(0,0,0,0.5); }
    .modal-header { display: flex; justify-content: space-between; align-items: center; padding: 16px; border-bottom: 1px solid var(--border); }
    .modal-header h3 { margin: 0; color: var(--text); }
    .close { color: var(--text-muted); font-size: 24px; font-weight: bold; cursor: pointer; line-height: 1; }
    .close:hover { color: var(--text); }
    .modal-body { padding: 16px; }
    .modal-footer { display: flex; gap: 8px; justify-content: flex-end; padding: 16px; border-top: 1px solid var(--border); }
    .checkbox-list { max-height: 200px; overflow-y: auto; border: 1px solid var(--border); border-radius: 6px; padding: 8px; background: var(--bg-main); }
    .checkbox-item { display: flex; align-items: center; padding: 6px 4px; margin: 2px 0; }
    .checkbox-item input[type="checkbox"] { margin: 0 8px 0 0; flex-shrink: 0; width: 16px; height: 16px; }
    .checkbox-item label { flex: 1; cursor: pointer; user-select: none; line-height: 1.3; word-wrap: break-word; }
    .btn-primary { background: var(--primary); }
  </style>
</head>
<body>
  <div id="loader" style="display: none;">
    <div class="spinner"></div>
  </div>
  
  <!-- Export Dialog -->
  <div id="export-modal" class="modal" style="display: none;">
    <div class="modal-content">
      <div class="modal-header">
        <h3>Export Table Descriptions to Excel</h3>
        <span class="close" onclick="hideExportDialog()">&times;</span>
      </div>
      <div class="modal-body">
        <label>Select databases to export:</label>
        <div id="database-checkboxes" class="checkbox-list">
          <!-- Database checkboxes will be populated here -->
        </div>
        <div style="margin-top: 16px;">
          <button onclick="selectAllDatabases()" class="btn-secondary btn-small">Select All</button>
          <button onclick="clearAllDatabases()" class="btn-secondary btn-small">Clear All</button>
        </div>
        <div class="info-box" style="margin-top: 12px; font-size: 12px;">
          <strong>ℹ️ Note:</strong> Browser may show security warnings for downloads over HTTP. This is normal in development mode.
        </div>
      </div>
      <div class="modal-footer">
        <button onclick="hideExportDialog()" class="btn-secondary">Cancel</button>
        <button onclick="exportToExcel()" class="btn-primary">Export</button>
      </div>
    </div>
  </div>
  
  <h1>🗄️ CHT Web Interface</h1>
  
  <div class="app-layout">
    <!-- Sidebar -->
    <div class="sidebar">
      <!-- Connection Management -->
      <div class="card card-compact">
        <div class="collapsible-header" onclick="toggleSection('connections')">
          <h2 style="margin: 0;">🔗 Connections</h2>
          <span class="chevron down" id="connections-chevron">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor">
              <path d="M7.41 8.58L12 13.17L16.59 8.58L18 10L12 16L6 10L7.41 8.58Z"/>
            </svg>
          </span>
        </div>
        <div class="collapsible-content expanded" id="connections-content">
          <div id="cluster-list" style="margin-bottom: 12px;">Loading...</div>
          
          <form id="cluster-form">
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px;">
              <div>
                <label for="cluster-name">Name</label>
                <input id="cluster-name" placeholder="my-cluster" required>
              </div>
              <div>
                <label for="cluster-host">Host</label>
                <input id="cluster-host" value="localhost" required>
              </div>
              <div>
                <label for="cluster-port">Port</label>
                <input id="cluster-port" type="number" value="8123" required>
              </div>
              <div>
                <label for="cluster-user">User</label>
                <input id="cluster-user" value="default">
              </div>
            </div>
            <div style="margin-bottom: 8px;">
              <label for="cluster-password">Password</label>
              <input id="cluster-password" type="password">
            </div>
            <div style="display: flex; gap: 8px; margin-bottom: 8px; font-size: 12px;">
              <label><input type="checkbox" id="cluster-readonly"> Read-only</label>
              <label><input type="checkbox" id="cluster-active" checked> Active</label>
            </div>
            <div style="display: flex; gap: 8px;">
              <button type="submit" class="btn-small" style="flex: 1;">Add cluster</button>
              <button type="button" onclick="testFrontend()" class="btn-secondary btn-small" title="Test frontend functionality">🧪 Test</button>
            </div>
          </form>
          
          <div style="margin-top: 12px; padding-top: 8px; border-top: 1px solid var(--border);">
            <div class="muted" style="margin-bottom: 4px;">💾 <span id="storage-count">0</span> saved connections</div>
            <button type="button" onclick="clearAllConnections()" class="btn-secondary btn-small" style="width: 100%;">Clear saved</button>
          </div>
        </div>
      </div>

      <!-- Database & Table Selection -->
      <div class="card card-compact">
        <h2>📊 Browse</h2>
        
        <div id="connection-help" class="error" style="display:none; margin-bottom: 12px;">
          <strong>No connection!</strong><br>
          Add a cluster above or start ClickHouse:<br>
          <code style="font-size: 11px;">docker run -p 8123:8123 clickhouse/clickhouse-server</code>
        </div>
        
        <div style="margin-bottom: 12px;">
          <label for="cluster-select">Cluster</label>
          <select id="cluster-select"></select>
        </div>

        <div style="margin-bottom: 12px;">
          <label for="db-select">Database</label>
          <select id="db-select"></select>
        </div>

        <div style="margin-bottom: 12px;">
          <button type="button" onclick="showExportDialog()" class="btn-secondary" style="width: 100%;" title="Export table descriptions to Excel">
            📊 Export to Excel
          </button>
        </div>

        <div>
          <label>Tables</label>
          <div class="muted" style="font-size: 11px; margin-bottom: 4px;">📋 Showing MergeTree tables only</div>
          <div id="tables" class="table-list"></div>
        </div>
      </div>
      
      <div id="status" class="status"></div>
    </div>

    <!-- Main Content -->
    <div class="main-content">
      <div class="card">
        <div id="table-detail">
          <div style="text-align: center; color: var(--muted); padding: 40px 20px;">
            <h3 style="color: var(--muted);">Select a table to view its schema</h3>
            <p class="muted">Choose a cluster, database, and table from the sidebar to see column details and edit comments.</p>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    const state = {
      clusters: [],
      activeCluster: null,
      databases: [],
      tables: [],
      selectedDb: null,
      selectedTable: null,
      editingCluster: null,
    };

    const qs = (sel) => document.querySelector(sel);
    let loadingCount = 0;

    // Basic obfuscation utilities (NOT encryption, just obfuscation)
    const obfuscate = {
      encode: (str) => {
        try {
          return btoa(str.split('').reverse().join(''));
        } catch(e) {
          return str;
        }
      },
      decode: (str) => {
        try {
          return atob(str).split('').reverse().join('');
        } catch(e) {
          return str;
        }
      }
    };

    // Connection storage with basic security measures
    const connectionStorage = {
      saveConnection: (connection) => {
        try {
          if (!connection.name || !connection.host) return false;
          
          const stored = JSON.parse(localStorage.getItem('cht-connections') || '{}');
          
          const safeConnection = {
            name: connection.name,
            host: connection.host,
            port: connection.port || 8123,
            user: connection.user || 'default',
            password: connection.password ? obfuscate.encode(connection.password) : '',
            read_only: connection.read_only || false,
            saved_at: new Date().toISOString()
          };
          
          stored[connection.name] = safeConnection;
          localStorage.setItem('cht-connections', JSON.stringify(stored));
          updateStorageCount();
          
          setStatus(`Connection '${connection.name}' saved to browser storage`, false, true);
          return true;
        } catch(e) {
          console.error('Failed to save connection:', e);
          setStatus('Failed to save connection to browser storage', true);
          return false;
        }
      },

      loadConnections: () => {
        try {
          const stored = JSON.parse(localStorage.getItem('cht-connections') || '{}');
          return Object.values(stored).map(conn => ({
            ...conn,
            password: conn.password ? obfuscate.decode(conn.password) : ''
          }));
        } catch(e) {
          console.error('Failed to load connections:', e);
          return [];
        }
      },

      clearAll: () => {
        try {
          localStorage.removeItem('cht-connections');
          updateStorageCount();
          setStatus('All saved connections cleared from browser storage', false, true);
          return true;
        } catch(e) {
          console.error('Failed to clear connections:', e);
          return false;
        }
      },

      count: () => {
        try {
          const stored = JSON.parse(localStorage.getItem('cht-connections') || '{}');
          return Object.keys(stored).length;
        } catch(e) {
          return 0;
        }
      }
    };

    function updateStorageCount() {
      const count = connectionStorage.count();
      qs('#storage-count').textContent = count;
    }

    function setLoading(isLoading) {
      if (isLoading) {
        loadingCount++;
      } else {
        loadingCount = Math.max(0, loadingCount - 1);
      }
      
      const loader = qs('#loader');
      const buttons = document.querySelectorAll('button');
      
      if (loadingCount > 0) {
        loader.style.display = 'flex';
        buttons.forEach(btn => btn.disabled = true);
        setStatus('Loading...', false, false);
      } else {
        loader.style.display = 'none';
        buttons.forEach(btn => btn.disabled = false);
      }
    }

    function setStatus(msg, isError=false, isSuccess=false) {
      const statusEl = qs('#status');
      if (statusEl) {
        statusEl.textContent = msg || '';
        statusEl.style.color = isError ? 'var(--danger)' : (isSuccess ? 'var(--success)' : 'var(--text-muted)');
        statusEl.style.display = 'block';
        statusEl.style.fontWeight = isError ? 'bold' : 'normal';
        
        // Always log status for debugging
        if (msg) {
          const prefix = isError ? '❌ ERROR:' : isSuccess ? '✅ SUCCESS:' : 'ℹ️ INFO:';
          console.log(`${prefix} ${msg}`);
          
          // Also show in browser console for immediate visibility
          if (isError) {
            console.error(msg);
          }
        }
        
        // Auto-clear status after 10 seconds unless it's an error
        if (!isError && msg) {
          setTimeout(() => {
            if (statusEl.textContent === msg) {
              statusEl.textContent = '';
            }
          }, 10000);
        }
      }
    }

    async function fetchJSON(url, options={}) {
      const timeout = 30000;
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeout);
      
      try {
        const res = await fetch(url, {
          ...options,
          signal: controller.signal
        });
        clearTimeout(timeoutId);
        
        if (!res.ok) {
          let message = res.statusText;
          try {
            const text = await res.text();
            try {
              const body = JSON.parse(text);
              message = body.detail || body.error || JSON.stringify(body);
            } catch {
              message = text || message;
            }
          } catch {
            // Fallback to status text if body reading fails
          }
          throw new Error(`${res.status} ${message}`);
        }
        if (res.status === 204) return null;
        return await res.json();
      } catch (error) {
        clearTimeout(timeoutId);
        if (error.name === 'AbortError') {
          throw new Error('Request timeout (30s)');
        }
        throw error;
      }
    }

    async function loadClusters() {
      setLoading(true);
      try {
        state.clusters = await fetchJSON('/clusters');
        console.log('Fetched clusters:', state.clusters);
        state.activeCluster = state.clusters.find(c => c.active)?.name || null;
        renderClusters();
        renderClusterSelect();
        
        if (state.clusters.length === 0) {
          qs('#connection-help').style.display = 'block';
        } else {
          qs('#connection-help').style.display = 'none';
        }
      } catch (e) {
        setStatus(`Error loading clusters: ${e.message}`, true);
        qs('#connection-help').style.display = 'block';
      } finally {
        setLoading(false);
      }
    }

    function renderClusters() {
      const html = state.clusters.map(c => `
        <div style="display: flex; justify-content: space-between; align-items: center; padding: 4px 8px; margin: 2px 0; border-radius: 4px; font-size: 12px; background: ${c.active ? 'var(--primary-light)' : 'var(--bg-gray)'};">
          <div>
            <strong>${c.name}</strong>
            <div class="muted">${c.host}:${c.port}</div>
          </div>
          <div style="display: flex; gap: 4px;">
            ${c.active ? '<span style="color: var(--success);">● Active</span>' : `<button onclick="activateCluster('${c.name}')" class="btn-secondary btn-small">Use</button>`}
            <button onclick="deleteCluster('${c.name}')" class="btn-secondary btn-small" style="color: var(--danger);">×</button>
          </div>
        </div>
      `).join('') || '<div class="muted">No clusters configured</div>';
      qs('#cluster-list').innerHTML = html;
      console.log('Rendered clusters HTML');
    }

    function renderClusterSelect() {
      const html = state.clusters.map(c => 
        `<option value="${c.name}" ${c.active ? 'selected' : ''}>${c.name} (${c.host}:${c.port})</option>`
      ).join('');
      qs('#cluster-select').innerHTML = html;
      qs('#cluster-select').value = state.activeCluster || '';
    }

    async function loadDatabases() {
      if (!state.activeCluster) return;
      
      setLoading(true);
      try {
        state.databases = await fetchJSON(`/databases?cluster=${encodeURIComponent(state.activeCluster)}`);
        renderDatabases();
      } catch (e) {
        setStatus(`Error loading databases: ${e.message}`, true);
      } finally {
        setLoading(false);
      }
    }

    function renderDatabases() {
      const html = state.databases.map(db => 
        `<option value="${db}">${db}</option>`
      ).join('');
      qs('#db-select').innerHTML = html;
      
      if (state.databases.length > 0) {
        state.selectedDb = state.databases[0];
        qs('#db-select').value = state.selectedDb;
        loadTables(state.selectedDb);
      }
    }

    async function loadTables(database) {
      if (!database || !state.activeCluster) return;
      
      setLoading(true);
      try {
        state.tables = await fetchJSON(`/databases/${database}/tables?cluster=${encodeURIComponent(state.activeCluster)}`);
        renderTables();
      } catch (e) {
        setStatus(`Error loading tables: ${e.message}`, true);
      } finally {
        setLoading(false);
      }
    }

    function renderTables() {
      if (state.tables.length === 0) {
        qs('#tables').innerHTML = '<div class="muted">No tables found</div>';
        return;
      }
      
      const html = state.tables.map(t => `
        <div class="table-item" data-table="${t.name}" onclick="selectTable('${t.name}')">
          <div style="font-weight: 500;">${t.name}</div>
          ${t.comment ? `<div class="muted">${t.comment}</div>` : ''}
        </div>
      `).join('');
      qs('#tables').innerHTML = html;
    }

    async function loadTableDetail(database, tableName) {
      if (!database || !tableName || !state.activeCluster) {
        renderTableDetail(null);
        return;
      }
      
      console.log(`Loading table details for ${database}.${tableName}`);
      setLoading(true);
      try {
        // Get table info from the tables list
        const tableInfo = state.tables.find(t => t.name === tableName);
        
        // Get column details
        const columns = await fetchJSON(`/databases/${encodeURIComponent(database)}/tables/${encodeURIComponent(tableName)}/columns?cluster=${encodeURIComponent(state.activeCluster)}`);
        console.log('Loaded columns:', columns);
        
        const detail = {
          database: database,
          table: tableName,
          comment: tableInfo?.comment || '',
          columns: columns || []
        };
        
        renderTableDetail(detail);
        setStatus(`Loaded ${columns.length} columns for ${tableName}`, false, true);
      } catch (e) {
        console.error('Error loading table details:', e);
        setStatus(`Error loading table details: ${e.message}`, true);
        renderTableDetail(null);
      } finally {
        setLoading(false);
      }
    }

    function renderTableDetail(detail) {
      const html = `
        <h2>${detail.database}.${detail.table}</h2>
        <div style="margin-bottom: 16px;">
          <label>Table Comment</label>
          <div style="display: flex; gap: 8px;">
            <input type="text" id="table-comment" value="${detail.comment || ''}" placeholder="Add table comment...">
            <button onclick="updateTableComment('${detail.database}', '${detail.table}')">Save</button>
          </div>
        </div>
        
        <h3>Columns (${detail.columns.length})</h3>
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Comment</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            ${detail.columns.map(col => `
              <tr>
                <td><strong>${col.name}</strong></td>
                <td><code>${col.type}</code></td>
                <td>
                  <input type="text" id="col-comment-${col.name}" value="${col.comment || ''}" 
                         placeholder="Add comment..." style="width: 100%; font-size: 12px;">
                </td>
                <td>
                  <button onclick="updateColumnComment('${detail.database}', '${detail.table}', '${col.name}')" 
                          class="btn-small">Save</button>
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      `;
      qs('#table-detail').innerHTML = html;
    }

    async function updateTableComment(database, table) {
      const comment = qs('#table-comment').value;
      setLoading(true);
      try {
        await fetchJSON(`/databases/${database}/tables/${table}/comment?cluster=${encodeURIComponent(state.activeCluster)}`, {
          method: 'PATCH',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ comment })
        });
        setStatus('Table comment updated successfully', false, true);
        loadTableDetail(database, table);
      } catch (e) {
        setStatus(`Error updating table comment: ${e.message}`, true);
      } finally {
        setLoading(false);
      }
    }

    async function updateColumnComment(database, table, column) {
      const comment = qs(`#col-comment-${column}`).value;
      setLoading(true);
      try {
        await fetchJSON(`/databases/${database}/tables/${table}/columns/${encodeURIComponent(column)}/comment?cluster=${encodeURIComponent(state.activeCluster)}`, {
          method: 'PATCH',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ comment })
        });
        setStatus('Column comment updated successfully', false, true);
        loadTableDetail(database, table);
      } catch (e) {
        setStatus(`Error updating column comment: ${e.message}`, true);
      } finally {
        setLoading(false);
      }
    }

    async function activateCluster(name) {
      setLoading(true);
      try {
        await fetchJSON(`/clusters/${name}/activate`, { method: 'POST' });
        setStatus(`Cluster '${name}' activated`, false, true);
        await loadClusters();
        await loadDatabases();
      } catch (e) {
        setStatus(`Error activating cluster: ${e.message}`, true);
      } finally {
        setLoading(false);
      }
    }

    async function deleteCluster(name) {
      if (!confirm(`Delete cluster '${name}'?`)) return;
      
      setLoading(true);
      try {
        await fetchJSON(`/clusters/${name}`, { method: 'DELETE' });
        setStatus(`Cluster '${name}' deleted`, false, true);
        await loadClusters();
        await loadDatabases();
      } catch (e) {
        setStatus(`Error deleting cluster: ${e.message}`, true);
      } finally {
        setLoading(false);
      }
    }

    function clearAllConnections() {
      if (!confirm('Clear all saved connections from browser storage?')) return;
      connectionStorage.clearAll();
    }

    // Frontend Testing Functions
    function testFrontend() {
      console.log('🧪 Starting frontend tests...');
      setStatus('Running frontend tests...', false, false);
      
      const tests = [
        { name: 'Status Display', test: () => {
          setStatus('Test message', false, false);
          const statusEl = qs('#status');
          return statusEl && statusEl.textContent === 'Test message';
        }},
        { name: 'Error Display', test: () => {
          setStatus('Test error', true, false);
          const statusEl = qs('#status');
          return statusEl && statusEl.textContent === 'Test error';
        }},
        { name: 'Loading State', test: () => {
          setLoading(true);
          const loader = qs('#loader');
          const isVisible = loader && loader.style.display === 'flex';
          setLoading(false);
          return isVisible;
        }},
        { name: 'Form Validation', test: () => {
          const nameInput = qs('#cluster-name');
          const hostInput = qs('#cluster-host');
          const portInput = qs('#cluster-port');
          return nameInput && hostInput && portInput;
        }},
        { name: 'API Connection', test: async () => {
          try {
            await fetchJSON('/clusters');
            return true;
          } catch (e) {
            console.log('API test failed (expected if no server):', e.message);
            return false;
          }
        }}
      ];
      
      let passed = 0;
      let total = tests.length;
      
      const runTests = async () => {
        for (const test of tests) {
          try {
            console.log(`Testing: ${test.name}`);
            const result = await test.test();
            if (result) {
              console.log(`✅ PASS: ${test.name}`);
              passed++;
            } else {
              console.log(`❌ FAIL: ${test.name}`);
            }
          } catch (e) {
            console.log(`❌ ERROR: ${test.name} - ${e.message}`);
          }
        }
        
        const summary = `Tests completed: ${passed}/${total} passed`;
        console.log(`🧪 ${summary}`);
        setStatus(summary, passed < total, passed === total);
      };
      
      runTests();
    }
    function toggleSection(sectionId) {
      const content = qs(`#${sectionId}-content`);
      const chevron = qs(`#${sectionId}-chevron`);
      
      if (content.classList.contains('collapsed')) {
        content.classList.remove('collapsed');
        content.classList.add('expanded');
        chevron.classList.add('down');
      } else {
        content.classList.remove('expanded');
        content.classList.add('collapsed');
        chevron.classList.remove('down');
      }
    }
    
    function selectTable(tableName) {
      // Remove previous selection
      document.querySelectorAll('.table-item').forEach(item => {
        item.classList.remove('selected');
      });
      
      // Add selection to clicked item
      const tableItem = document.querySelector(`[data-table="${tableName}"]`);
      if (tableItem) {
        tableItem.classList.add('selected');
      }
      
      state.selectedTable = tableName;
      loadTableDetail(state.selectedDb, tableName);
    }

    // Event Listeners
    qs('#cluster-select').addEventListener('change', async (e) => {
      const clusterName = e.target.value;
      if (clusterName && clusterName !== state.activeCluster) {
        await activateCluster(clusterName);
      }
    });

    qs('#db-select').addEventListener('change', (e) => {
      state.selectedDb = e.target.value;
      if (state.selectedDb) {
        loadTables(state.selectedDb);
      }
    });

    qs('#cluster-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      
      // Validate form inputs
      const name = qs('#cluster-name').value.trim();
      const host = qs('#cluster-host').value.trim();
      const port = Number(qs('#cluster-port').value);
      
      if (!name) {
        setStatus('Error: Cluster name is required', true);
        return;
      }
      if (!host) {
        setStatus('Error: Host is required', true);
        return;
      }
      if (!port || port < 1 || port > 65535) {
        setStatus('Error: Valid port number is required (1-65535)', true);
        return;
      }
      
      const payload = {
        name: name,
        host: host,
        port: port,
        user: qs('#cluster-user').value.trim() || 'default',
        password: qs('#cluster-password').value,
        read_only: qs('#cluster-readonly').checked,
        make_active: qs('#cluster-active').checked,
      };
      
      console.log('Submitting cluster:', payload);
      setLoading(true);
      setStatus('Adding cluster...', false, false);
      
      try {
        if (state.editingCluster) {
          setStatus(`Updating cluster '${state.editingCluster}'...`, false, false);
          const result = await fetchJSON(`/clusters/${state.editingCluster}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload),
          });
          console.log('Update result:', result);
          setStatus(`Cluster '${state.editingCluster}' updated successfully!`, false, true);
          state.editingCluster = null;
        } else {
          const result = await fetchJSON('/clusters', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload),
          });
          console.log('Add result:', result);
          setStatus(`Cluster '${payload.name}' added successfully!`, false, true);
          
          // Auto-save to browser storage
          connectionStorage.saveConnection(payload);
          
          // Reset form
          qs('#cluster-form').reset();
          qs('#cluster-port').value = 8123;
          qs('#cluster-active').checked = true;
        }
        
        // Reload clusters and databases
        console.log('Reloading clusters...');
        await loadClusters();
        console.log('Reloading databases...');
        await loadDatabases();
        console.log('Cluster operation completed successfully');
        
      } catch (e) {
        console.error('Cluster operation failed:', e);
        const action = state.editingCluster ? 'updating' : 'adding';
        let errorMsg = `Error ${action} cluster: `;
        
        if (e.message.includes('timeout')) {
          errorMsg += 'Request timed out. Check your network connection.';
        } else if (e.message.includes('fetch')) {
          errorMsg += 'Network error. Is the server running?';
        } else if (e.message.includes('400')) {
          errorMsg += 'Invalid cluster configuration.';
        } else if (e.message.includes('409')) {
          errorMsg += 'Cluster name already exists.';
        } else if (e.message.includes('500')) {
          errorMsg += 'Server error. Check server logs.';
        } else {
          errorMsg += e.message || 'Unknown error occurred';
        }
        
        setStatus(errorMsg, true);
      } finally {
        setLoading(false);
        console.log('Loading state cleared');
      }
    });

    // Initial load with auto-restore from storage
    async function initialize() {
      updateStorageCount();
      
      // Auto-load saved connections first
      const stored = connectionStorage.loadConnections();
      if (stored.length > 0) {
        setStatus('Loading saved connections...', false, false);
        try {
          let loaded = 0;
          for (const conn of stored) {
            try {
              await fetchJSON('/clusters', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  name: conn.name,
                  host: conn.host,
                  port: conn.port,
                  user: conn.user,
                  password: conn.password,
                  read_only: conn.read_only,
                  make_active: loaded === 0, // Make first one active
                })
              });
              loaded++;
            } catch (e) {
              console.error(`Failed to load connection '${conn.name}':`, e);
            }
          }
          if (loaded > 0) {
            setStatus(`Auto-loaded ${loaded} saved connection(s)`, false, true);
          }
        } catch (e) {
          console.error('Error loading saved connections:', e);
        }
      }
      
      await loadClusters();
      await loadDatabases();
    }

    // Export functionality
    function showExportDialog() {
      if (!state.activeCluster) {
        setStatus('Please select an active cluster first', true);
        return;
      }
      
      if (!state.databases || state.databases.length === 0) {
        setStatus('No databases available for export', true);
        return;
      }
      
      // Populate database checkboxes
      const checkboxContainer = qs('#database-checkboxes');
      checkboxContainer.innerHTML = state.databases.map(db => `
        <div class="checkbox-item">
          <input type="checkbox" id="db-${db}" value="${db}">
          <label for="db-${db}">${db}</label>
        </div>
      `).join('');
      
      // Show modal
      qs('#export-modal').style.display = 'flex';
    }

    function hideExportDialog() {
      qs('#export-modal').style.display = 'none';
    }

    function selectAllDatabases() {
      const checkboxes = document.querySelectorAll('#database-checkboxes input[type="checkbox"]');
      checkboxes.forEach(cb => cb.checked = true);
    }

    function clearAllDatabases() {
      const checkboxes = document.querySelectorAll('#database-checkboxes input[type="checkbox"]');
      checkboxes.forEach(cb => cb.checked = false);
    }

    async function exportToExcel() {
      const selectedDatabases = Array.from(
        document.querySelectorAll('#database-checkboxes input[type="checkbox"]:checked')
      ).map(cb => cb.value);
      
      if (selectedDatabases.length === 0) {
        setStatus('Please select at least one database to export', true);
        return;
      }
      
      try {
        setStatus(`Exporting ${selectedDatabases.length} database(s) to Excel...`, false);
        setLoading(true);
        
        const response = await fetch('/databases/export/excel', {
          method: 'POST',
          headers: { 
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
          },
          body: JSON.stringify({
            databases: selectedDatabases,
            cluster: state.activeCluster
          })
        });
        
        if (!response.ok) {
          let errorMessage;
          try {
            const errorData = await response.json();
            errorMessage = errorData.detail || `Export failed: ${response.status}`;
          } catch {
            errorMessage = `Export failed: ${response.status} ${response.statusText}`;
          }
          throw new Error(errorMessage);
        }
        
        // Get response headers for filename
        const contentDisposition = response.headers.get('Content-Disposition');
        const contentLength = response.headers.get('Content-Length');
        console.log('📊 Response headers:', { contentDisposition, contentLength });
        
        // Extract filename from Content-Disposition header
        let filename = 'table_descriptions.xlsx';
        if (contentDisposition) {
          const filenameMatch = contentDisposition.match(/filename=([^;]+)/);
          if (filenameMatch) {
            filename = filenameMatch[1].replace(/["']/g, '');
          }
        }
        
        console.log(`📊 Downloading file: ${filename}`);
        setStatus('Processing Excel file...', false);
        
        // Get the response as array buffer for better control
        const arrayBuffer = await response.arrayBuffer();
        console.log(`📊 Excel file: ${arrayBuffer.byteLength} bytes`);
        
        // Validate file size
        if (arrayBuffer.byteLength === 0) {
          throw new Error('Received empty file from server');
        }
        
        if (arrayBuffer.byteLength < 1000) {
          console.warn('⚠️ Suspiciously small Excel file, might be an error response');
        }
        
        setStatus('Starting download...', false);
        
        // Create blob from array buffer with explicit type
        const blob = new Blob([arrayBuffer], {
          type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        });
        
        // Try multiple download methods for better browser compatibility
        let downloadSuccess = false;
        
        // Method 1: Modern approach with proper timing
        try {
          console.log('📊 Attempting modern download method...');
          
          const url = URL.createObjectURL(blob);
          
          // Create a temporary link element
          const link = document.createElement('a');
          link.href = url;
          link.download = filename;
          link.style.display = 'none';
          
          // Add to document
          document.body.appendChild(link);
          
          // Wait for next tick to ensure element is in DOM
          await new Promise(resolve => setTimeout(resolve, 50));
          
          // Trigger download
          link.click();
          
          // Wait a bit before cleanup to ensure download starts
          await new Promise(resolve => setTimeout(resolve, 500));
          
          // Cleanup immediately after delay
          document.body.removeChild(link);
          
          // Delay URL revocation significantly for macOS
          setTimeout(() => {
            URL.revokeObjectURL(url);
            console.log('📊 Download URL revoked');
          }, 30000); // 30 second delay for slow downloads
          
          downloadSuccess = true;
          
        } catch (methodError) {
          console.warn('Modern download method failed:', methodError);
        }
        
        // Method 2: Fallback using data URL (for problematic browsers)
        if (!downloadSuccess) {
          try {
            console.log('📊 Attempting data URL fallback method...');
            
            const reader = new FileReader();
            const dataUrlPromise = new Promise((resolve, reject) => {
              reader.onload = () => resolve(reader.result);
              reader.onerror = reject;
            });
            
            reader.readAsDataURL(blob);
            const dataUrl = await dataUrlPromise;
            
            const link = document.createElement('a');
            link.href = dataUrl;
            link.download = filename;
            link.style.display = 'none';
            
            document.body.appendChild(link);
            await new Promise(resolve => setTimeout(resolve, 50));
            link.click();
            await new Promise(resolve => setTimeout(resolve, 500));
            document.body.removeChild(link);
            
            downloadSuccess = true;
            
          } catch (fallbackError) {
            console.error('Data URL fallback failed:', fallbackError);
          }
        }
        
        if (!downloadSuccess) {
          throw new Error('All download methods failed. Please try again or check browser settings.');
        }
        
        setStatus(`Successfully exported ${selectedDatabases.length} database(s) to Excel`, false, true);
        hideExportDialog();
        
        // Show helpful info about download process
        setTimeout(() => {
          setStatus('📁 Download started! Check your Downloads folder.', false, false);
        }, 1000);
        
        setTimeout(() => {
          if (navigator.userAgent.includes('Mac')) {
            setStatus('💡 macOS: If download shows as "Не подтверждён", please wait for it to complete', false, false);
          } else {
            setStatus('💡 If download doesn\'t appear, check browser\'s download blocking settings', false, false);
          }
        }, 3000);
        
      } catch (error) {
        console.error('Export error:', error);
        setStatus(`Export failed: ${error.message}`, true);
        hideExportDialog();
        
        // Show detailed error for debugging
        if (error.message.includes('All download methods failed')) {
          alert(`Download Error: ${error.message}\n\nTip: Try right-clicking and "Save As..." or check browser download settings.`);
        }
      } finally {
        setLoading(false);
      }
    }
    
    initialize();
    
    // Global error handler
    window.addEventListener('error', (e) => {
      console.error('JavaScript error:', e);
      setStatus(`JavaScript error: ${e.error?.message || e.message || 'Unknown error'}`, true);
    });
    
    window.addEventListener('unhandledrejection', (e) => {
      console.error('Unhandled promise rejection:', e);
      setStatus(`Promise error: ${e.reason?.message || e.reason || 'Unknown error'}`, true);
    });
  </script>
</body>
</html>"""
