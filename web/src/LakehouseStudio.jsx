import { useEffect, useState } from "react";

function readLocalJson(key, fallback) {
  try { const value = localStorage.getItem(key); return value ? JSON.parse(value) : fallback; }
  catch { return fallback; }
}
const API = "/api/v1";

async function request(path, options) {
  const response = await fetch(API + path, {
    headers: { "Content-Type": "application/json", ...(options?.headers || {}) },
    ...options
  });
  const body = await response.text();
  let data;
  try { data = body ? JSON.parse(body) : {}; } catch { data = { message: body }; }
  if (!response.ok) throw new Error(data.error || data.message || ("HTTP " + response.status));
  return data;
}

function DataGrid({ rows, columns }) {
  if (!rows?.length) return <div className="empty-state">No rows returned.</div>;
  const keys = columns?.length ? columns : Object.keys(rows[0]);
  return <div className="data-grid"><table><thead><tr>{keys.map(k => <th key={k}>{k}</th>)}</tr></thead>
    <tbody>{rows.map((row, i) => <tr key={i}>{keys.map(k => <td key={k}>{typeof row[k] === "object" ? JSON.stringify(row[k]) : String(row[k] ?? "")}</td>)}</tr>)}</tbody></table></div>;
}

function Results({ result }) {
  if (!result) return null;
  return <div className="query-result">
    <div className="result-meta"><span>{result.rowCount ?? result.rows?.length ?? 0} rows</span><span>{result.engine || "Spark SQL"}</span></div>
    <DataGrid rows={result.rows || []} columns={result.columns || []}/>
  </div>;
}

function SqlEditor({ value, onChange, onRun, busy, compact = false }) {
  return <div className={"sql-editor " + (compact ? "compact" : "")}>
    <div className="editor-toolbar"><span>SQL · Spark SQL / Iceberg</span><button className="studio-primary" onClick={onRun} disabled={busy}>{busy ? "Running…" : "▶ Run query"}</button></div>
    <textarea spellCheck="false" aria-label="SQL query editor" value={value} onChange={e => onChange(e.target.value)} />
  </div>;
}

function SqlWorkspace() {
  const [sql, setSql] = useState("SELECT current_timestamp() AS executed_at, current_database() AS catalog;");
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [history, setHistory] = useState(() => readLocalJson("sbase.sql.history", []));
  async function run(query = sql) {
    setBusy(true); setError(""); setResult(null);
    const started = performance.now();
    try {
      const data = await request("/engines/spark/sql", { method: "POST", body: JSON.stringify({ sql: query, limit: 1000 }) });
      setResult({ ...data, elapsedMs: Math.round(performance.now() - started) });
      const next = [{ sql: query, at: new Date().toISOString(), rows: data.rowCount || 0 }, ...history.filter(h => h.sql !== query)].slice(0, 20);
      setHistory(next); localStorage.setItem("sbase.sql.history", JSON.stringify(next));
    } catch (e) { setError(e.message); }
    finally { setBusy(false); }
  }
  return <div className="studio-panel">
    <div className="studio-heading"><div><div className="section-title">SQL WORKSPACE</div><h2>Query editor</h2><p>Run read-only SQL against the configured Spark and Iceberg catalog.</p></div><span className="engine-pill">Spark SQL</span></div>
    <div className="sql-layout"><div className="sql-main">
      <SqlEditor value={sql} onChange={setSql} onRun={() => run()} busy={busy}/>
      {error && <div className="studio-error">{error}</div>}
      {result && <><div className="result-meta"><span>{result.elapsedMs} ms</span><span>{result.rowCount} rows · limit {result.limit}</span></div><Results result={result}/></>}
    </div><aside className="history-panel"><div className="section-title">QUERY HISTORY</div>{history.length ? history.map((h, i) => <button className="history-item" key={h.at + i} onClick={() => setSql(h.sql)}><code>{h.sql}</code><small>{new Date(h.at).toLocaleString()} · {h.rows} rows</small></button>) : <p className="muted">Executed queries will appear here.</p>}</aside></div>
  </div>;
}

function LakehouseBrowser() {
  const [status, setStatus] = useState(null);
  const [namespaces, setNamespaces] = useState([]);
  const [namespace, setNamespace] = useState("");
  const [tables, setTables] = useState([]);
  const [selected, setSelected] = useState("");
  const [preview, setPreview] = useState(null);
  const [schema, setSchema] = useState(null);
  const [snapshots, setSnapshots] = useState([]);
  const [error, setError] = useState("");
  const [newNamespace, setNewNamespace] = useState("");
  async function load() {
    setError("");
    try {
      const [s, ns] = await Promise.all([request("/lakehouse"), request("/lakehouse/namespaces")]);
      setStatus(s);
      const list = Array.isArray(ns) ? ns : ns.namespaces || [];
      setNamespaces(list.map(v => typeof v === "string" ? v : v.name || v.namespace).filter(Boolean));
    } catch (e) { setError(e.message); }
  }
  useEffect(() => { load(); }, []);
  useEffect(() => {
    if (!namespace) { setTables([]); return; }
    request("/lakehouse/namespaces/" + encodeURIComponent(namespace) + "/tables")
      .then(v => setTables((Array.isArray(v) ? v : v.tables || []).map(t => typeof t === "string" ? t : t.name || t.table)))
      .catch(e => setError(e.message));
  }, [namespace]);
  async function inspect(table) {
    setSelected(table); setError(""); setPreview(null); setSchema(null); setSnapshots([]);
    const base = "/lakehouse/namespaces/" + encodeURIComponent(namespace) + "/tables/" + encodeURIComponent(table);
    try {
      const [p, s, history] = await Promise.all([request(base + "?limit=100"), request(base + "/schema"), request(base + "/snapshots")]);
      setPreview(p); setSchema(s); setSnapshots(Array.isArray(history) ? history : history.snapshots || []);
    } catch (e) { setError(e.message); }
  }
  async function compactTable() {
    try { await request("/lakehouse/namespaces/" + encodeURIComponent(namespace) + "/tables/" + encodeURIComponent(selected) + "/compact", { method: "POST" }); await inspect(selected); }
    catch (e) { setError(e.message); }
  }
  async function rollback(snapshotId) {
    if (!window.confirm("Roll back this table to snapshot " + snapshotId + "?")) return;
    try { await request("/lakehouse/namespaces/" + encodeURIComponent(namespace) + "/tables/" + encodeURIComponent(selected) + "/snapshots/" + encodeURIComponent(snapshotId) + "/rollback", { method: "POST" }); await inspect(selected); }
    catch (e) { setError(e.message); }
  }
  async function createNamespace() {
    if (!newNamespace.trim()) return;
    try { await request("/lakehouse/namespaces", { method: "POST", body: JSON.stringify({ namespace: newNamespace.trim() }) }); setNewNamespace(""); await load(); }
    catch (e) { setError(e.message); }
  }
  return <div className="studio-panel">
    <div className="studio-heading"><div><div className="section-title">LAKEHOUSE EXPLORER</div><h2>Catalog browser</h2><p>Explore Iceberg namespaces, table metadata, schemas and sample records.</p></div><button onClick={load}>↻ Refresh</button></div>
    {status && <div className="lakehouse-status"><span className={status.status === "UP" ? "health-dot" : "health-dot warning"}/><b>{status.status || "UNKNOWN"}</b><span>Catalog: {status.catalogStatus || "—"}</span><span>Object store: {status.objectStoreStatus || "—"}</span></div>}
    {error && <div className="studio-error">{error}</div>}
    <div className="catalog-layout"><aside className="catalog-tree"><div className="section-title">NAMESPACES</div>{namespaces.map(n => <button className={namespace === n ? "tree-item active" : "tree-item"} key={n} onClick={() => { setNamespace(n); setSelected(""); setPreview(null); }}>▸ {n}</button>)}<div className="new-namespace"><input value={newNamespace} onChange={e => setNewNamespace(e.target.value)} placeholder="New namespace"/><button onClick={createNamespace}>+</button></div>{namespace && <><div className="section-title table-title">TABLES</div>{tables.map(t => <button className={selected === t ? "tree-item active" : "tree-item"} key={t} onClick={() => inspect(t)}>▤ {t}</button>)}</>}</aside>
      <div className="catalog-detail">{selected ? <><div className="detail-title"><h3>{namespace}.{selected}</h3><span>Apache Iceberg</span><button onClick={compactTable}>Compact files</button></div><div className="detail-tabs"><section><div className="section-title">SCHEMA / DDL</div><pre>{schema ? JSON.stringify(schema, null, 2) : "Loading schema…"}</pre></section><section><div className="section-title">DATA PREVIEW</div><DataGrid rows={preview?.rows || preview?.data || []} columns={preview?.columns || []}/></section></div><section className="snapshot-panel"><div className="section-title">SNAPSHOT HISTORY</div>{snapshots.length ? snapshots.map((snap, i) => <div className="snapshot-row" key={snap.snapshotId || snap.id || i}><code>{snap.snapshotId || snap.id || JSON.stringify(snap)}</code><span>{snap.timestamp || snap.committedAt || ""}</span><button onClick={() => rollback(snap.snapshotId || snap.id)}>Restore</button></div>) : <div className="empty-state">No snapshots reported for this table.</div>}</section></> : <div className="empty-state">Select a namespace and table to inspect its schema and records.</div>}</div>
    </div>
  </div>;
}

function Notebook() {
  const initialCells = [{ id: 1, type: "sql", source: "SELECT current_timestamp() AS now;", output: null }, { id: 2, type: "markdown", source: "# Lakehouse analysis\nDescribe your analysis here.", output: null }];
  const [token, setToken] = useState(() => localStorage.getItem("sbase.notebook.token") || "");
  const [tokenInput, setTokenInput] = useState(() => localStorage.getItem("sbase.notebook.token") || "");
  const [notebooks, setNotebooks] = useState([]);
  const [notebookId, setNotebookId] = useState("");
  const [title, setTitle] = useState("Untitled lakehouse notebook");
  const [description, setDescription] = useState("");
  const [cells, setCells] = useState(initialCells);
  const [version, setVersion] = useState(0);
  const [permission, setPermission] = useState("OWNER");
  const [shares, setShares] = useState([]);
  const [revisions, setRevisions] = useState([]);
  const [sharePermission, setSharePermission] = useState("READ");
  const [shareLabel, setShareLabel] = useState("");
  const [newShareToken, setNewShareToken] = useState("");
  const [dirty, setDirty] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  function authHeaders(value = token) { return value ? { "X-Notebook-Token": value } : {}; }
  function parseCells(value) {
    try { const parsed = typeof value === "string" ? JSON.parse(value) : value; return Array.isArray(parsed) ? parsed : initialCells; }
    catch { return initialCells; }
  }
  async function refreshList(value = token) {
    if (!value) { setNotebooks([]); return; }
    const list = await request("/notebooks", { headers: authHeaders(value) });
    setNotebooks(list);
  }
  async function openNotebook(id, value = token) {
    setBusy(true); setError(""); setNotice("");
    try {
      const [data, history] = await Promise.all([
        request("/notebooks/" + encodeURIComponent(id), { headers: authHeaders(value) }),
        request("/notebooks/" + encodeURIComponent(id) + "/revisions", { headers: authHeaders(value) })
      ]);
      setNotebookId(data.id); setTitle(data.title); setDescription(data.description || "");
      setCells(parseCells(data.cells)); setVersion(Number(data.version)); setPermission(data.actor || "SHARE");
      setRevisions(history); setShares([]);
      if (data.actor === "OWNER") {
        try { setShares(await request("/notebooks/" + encodeURIComponent(id) + "/shares", { headers: authHeaders(value) })); } catch {}
      }
      setDirty(false);
    } catch (e) { setError(e.message); }
    finally { setBusy(false); }
  }
  async function connectToken() {
    const value = tokenInput.trim();
    if (!value) return;
    setBusy(true); setError("");
    try {
      await refreshList(value);
      setToken(value); localStorage.setItem("sbase.notebook.token", value);
      setNotice("Notebook access connected.");
      setNotebookId("");
    } catch (e) { setError(e.message); }
    finally { setBusy(false); }
  }
  async function createNotebook() {
    setBusy(true); setError(""); setNotice("");
    try {
      const result = await request("/notebooks", { method: "POST", body: JSON.stringify({ title: "Untitled lakehouse notebook", description: "", cells: initialCells.map(({id,type,source}) => ({id,type,source})) }) });
      const access = result.accessToken;
      setToken(access); setTokenInput(access); localStorage.setItem("sbase.notebook.token", access);
      await refreshList(access);
      const notebook = result.notebook;
      setNotebookId(notebook.id); setTitle(notebook.title); setDescription(""); setCells(parseCells(notebook.cells));
      setVersion(Number(notebook.version)); setPermission("OWNER"); setShares([]); setRevisions([]); setDirty(false);
      setNotice("Notebook created. Your owner access token is stored in this browser; keep a backup before clearing browser data.");
    } catch (e) { setError(e.message); }
    finally { setBusy(false); }
  }
  async function saveNotebook() {
    if (!notebookId || permission === "SHARE" && !token) return;
    setBusy(true); setError(""); setNotice("");
    try {
      const payload = { expectedVersion: version, title, description, cells: cells.map(({id,type,source}) => ({id,type,source})), changeSummary: "Saved from Lakehouse Studio" };
      const saved = await request("/notebooks/" + encodeURIComponent(notebookId), { method: "PUT", headers: authHeaders(), body: JSON.stringify(payload) });
      setVersion(Number(saved.version)); setDirty(false); setNotice("Saved as version " + saved.version + ".");
      await refreshList();
      await openNotebook(notebookId);
    } catch (e) { setError(e.message); }
    finally { setBusy(false); }
  }
  async function createShare() {
    if (!notebookId) return;
    setBusy(true); setError(""); setNewShareToken("");
    try {
      const share = await request("/notebooks/" + encodeURIComponent(notebookId) + "/shares", { method: "POST", headers: authHeaders(), body: JSON.stringify({ permission: sharePermission, label: shareLabel || null }) });
      setNewShareToken(share.token); setShareLabel("");
      setShares(await request("/notebooks/" + encodeURIComponent(notebookId) + "/shares", { headers: authHeaders() }));
      setNotice("Share token created. It is shown only once; copy it and send it securely.");
    } catch (e) { setError(e.message); }
    finally { setBusy(false); }
  }
  async function revokeShare(shareId) {
    if (!window.confirm("Revoke this notebook share?")) return;
    try {
      await request("/notebooks/" + encodeURIComponent(notebookId) + "/shares/" + encodeURIComponent(shareId) + "/revoke", { method: "POST", headers: authHeaders() });
      setShares(await request("/notebooks/" + encodeURIComponent(notebookId) + "/shares", { headers: authHeaders() }));
    } catch (e) { setError(e.message); }
  }
  async function deleteNotebook() {
    if (!window.confirm("Delete this notebook? Its revisions and share links will also be removed.")) return;
    try {
      await request("/notebooks/" + encodeURIComponent(notebookId), { method: "DELETE", headers: authHeaders() });
      setNotebookId(""); setCells(initialCells); setVersion(0); await refreshList(); setNotice("Notebook deleted.");
    } catch (e) { setError(e.message); }
  }
  function update(id, patch) { setCells(current => current.map(cell => cell.id === id ? { ...cell, ...patch } : cell)); setDirty(true); }
  async function execute(cell) {
    if (cell.type !== "sql") return;
    setBusy(true); setError("");
    try {
      const output = await request("/engines/spark/sql", { method: "POST", body: JSON.stringify({ sql: cell.source, limit: 1000 }) });
      update(cell.id, { output: { ...output, ranAt: new Date().toISOString() } });
    } catch (e) { setError("Cell " + cell.id + ": " + e.message); update(cell.id, { output: { error: e.message } }); }
    finally { setBusy(false); }
  }
  async function runAll() {
    setBusy(true); setError("");
    for (const cell of cells) {
      if (cell.type !== "sql") continue;
      try {
        const output = await request("/engines/spark/sql", { method: "POST", body: JSON.stringify({ sql: cell.source, limit: 1000 }) });
        setCells(current => current.map(item => item.id === cell.id ? { ...item, output: { ...output, ranAt: new Date().toISOString() } } : item));
      } catch (e) { setError("Cell " + cell.id + ": " + e.message); break; }
    }
    setBusy(false);
  }
  function move(index, delta) {
    const target = index + delta; if (target < 0 || target >= cells.length) return;
    const copy = [...cells]; [copy[index], copy[target]] = [copy[target], copy[index]]; setCells(copy); setDirty(true);
  }
  function add(type) { setCells(current => [...current, { id: Date.now() + Math.random(), type, source: type === "sql" ? "SELECT * FROM sbase.default.sample_table LIMIT 100;" : "## Notes\nWrite analysis notes here.", output: null }]); setDirty(true); }

  return <div className="studio-panel notebook-panel">
    <div className="studio-heading"><div><div className="section-title">PERSISTENT NOTEBOOKS</div><h2>Collaborative lakehouse notebook</h2><p>PostgreSQL persistence · immutable revisions · revocable capability sharing · optimistic concurrency</p></div><div className="notebook-actions"><button onClick={createNotebook} disabled={busy}>+ New notebook</button></div></div>
    <div className="notebook-connect">
      <input value={tokenInput} onChange={e => setTokenInput(e.target.value)} placeholder="Paste owner or shared notebook access token"/>
      <button onClick={connectToken} disabled={busy || !tokenInput.trim()}>Connect</button>
      <span>{token ? "Access token connected" : "Create a notebook or connect with a shared token"}</span>
    </div>
    {notebooks.length > 0 && <div className="notebook-library"><b>MY / SHARED NOTEBOOKS</b>{notebooks.map(item => <button key={item.id} className={notebookId === item.id ? "active" : ""} onClick={() => openNotebook(item.id)}><span>{item.title}</span><small>v{item.version} · {item.permission} · {new Date(item.updatedAt).toLocaleString()}</small></button>)}</div>}
    {error && <div className="studio-error">{error}</div>}
    {notice && <div className="notebook-notice">{notice}</div>}
    {notebookId ? <>
      <div className="notebook-document-bar"><div><input className="notebook-title-input" value={title} onChange={e => {setTitle(e.target.value);setDirty(true);}}/><input className="notebook-description-input" value={description} onChange={e => {setDescription(e.target.value);setDirty(true);}} placeholder="Description (optional)"/></div><div className="notebook-actions"><span className="engine-pill">{permission} · v{version}</span><button className="studio-primary" onClick={saveNotebook} disabled={busy || !dirty || permission === "SHARE"}>{busy ? "Working…" : dirty ? "Save changes" : "Saved"}</button>{permission === "OWNER" && <button onClick={deleteNotebook}>Delete</button>}</div></div>
      <div className="notebook-actions notebook-cell-actions"><button onClick={() => add("markdown")}>+ Markdown</button><button onClick={() => add("sql")}>+ SQL cell</button><button className="studio-primary" onClick={runAll} disabled={busy}>▶ Run all</button></div>
      {cells.map((cell, index) => <article className="notebook-cell" key={cell.id}><div className="cell-gutter"><b>{String(index + 1).padStart(2, "0")}</b><span>{cell.type.toUpperCase()}</span><button title="Move up" onClick={() => move(index, -1)} disabled={!index || permission === "SHARE"}>↑</button><button title="Move down" onClick={() => move(index, 1)} disabled={index === cells.length - 1 || permission === "SHARE"}>↓</button><button title="Delete cell" disabled={permission === "SHARE"} onClick={() => {setCells(current => current.filter(item => item.id !== cell.id));setDirty(true);}}>×</button></div><div className="cell-body"><textarea aria-label={cell.type + " cell"} spellCheck={cell.type === "markdown"} readOnly={permission === "SHARE"} value={cell.source} onChange={e => update(cell.id, { source: e.target.value })}/>{cell.type === "sql" && <button className="cell-run" disabled={busy} onClick={() => execute(cell)}>▶ Run cell</button>}{cell.type === "markdown" && <pre className="markdown-preview">{cell.source}</pre>}{cell.output && (cell.output.error ? <div className="studio-error">{cell.output.error}</div> : <Results result={cell.output}/> )}</div></article>)}
      {permission === "OWNER" && <div className="notebook-sharing"><div className="section-title">SHARE ACCESS</div><div className="share-create"><select value={sharePermission} onChange={e => setSharePermission(e.target.value)}><option value="READ">Read only</option><option value="WRITE">Can edit</option></select><input value={shareLabel} onChange={e => setShareLabel(e.target.value)} placeholder="Share label (e.g. Analytics team)"/><button onClick={createShare} disabled={busy}>Create share</button></div>{newShareToken && <div className="share-token"><code>{newShareToken}</code><button onClick={() => navigator.clipboard?.writeText(newShareToken)}>Copy token</button></div>}{shares.map(share => <div className="share-row" key={share.shareId}><span>{share.label || "Shared access"} · {share.permission} · {share.revokedAt ? "REVOKED" : "ACTIVE"}</span>{!share.revokedAt && <button onClick={() => revokeShare(share.shareId)}>Revoke</button>}</div>)}</div>}
      <div className="notebook-revisions"><div className="section-title">VERSION HISTORY</div>{revisions.map(revision => <div key={revision.version}><span>v{revision.version} · {revision.summary || revision.title}</span><small>{new Date(revision.changedAt).toLocaleString()}</small></div>)}</div>
    </> : <div className="empty-state">Create a notebook or connect with a token to open a notebook shared with you.</div>}
    <div className="notebook-footer">Notebook content and revision history are stored in PostgreSQL. Access tokens are hashed at rest. SQL execution uses the SBase Spark engine. {dirty ? "Unsaved changes." : ""}</div>
  </div>;
}

export default function LakehouseStudio() {
  const [view, setView] = useState("lakehouse");
  return <section className="lakehouse-studio">
    <nav className="studio-nav"><div className="studio-brand"><span>◈</span> LAKEHOUSE STUDIO</div>{[["lakehouse","Catalog"],["sql","SQL Workspace"],["notebook","Notebook"]].map(([id, label]) => <button key={id} className={view === id ? "selected" : ""} onClick={() => setView(id)}>{label}</button>)}</nav>
    {view === "lakehouse" ? <LakehouseBrowser/> : view === "sql" ? <SqlWorkspace/> : <Notebook/>}
  </section>;
}
