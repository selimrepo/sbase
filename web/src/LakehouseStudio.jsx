import { useEffect, useMemo, useState } from "react";

const DEFAULT_SQL = "SELECT * FROM sbase.default.sample_table LIMIT 100;";
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
  const [history, setHistory] = useState(() => JSON.parse(localStorage.getItem("sbase.sql.history") || "[]"));
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
    setSelected(table); setError(""); setPreview(null); setSchema(null);
    const base = "/lakehouse/namespaces/" + encodeURIComponent(namespace) + "/tables/" + encodeURIComponent(table);
    try {
      const [p, s] = await Promise.all([request(base + "?limit=100"), request(base + "/schema")]);
      setPreview(p); setSchema(s);
    } catch (e) { setError(e.message); }
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
      <div className="catalog-detail">{selected ? <><div className="detail-title"><h3>{namespace}.{selected}</h3><span>Apache Iceberg</span></div><div className="detail-tabs"><section><div className="section-title">SCHEMA / DDL</div><pre>{schema ? JSON.stringify(schema, null, 2) : "Loading schema…"}</pre></section><section><div className="section-title">DATA PREVIEW</div><DataGrid rows={preview?.rows || preview?.data || []} columns={preview?.columns || []}/></section></div></> : <div className="empty-state">Select a namespace and table to inspect its schema and records.</div>}</div>
    </div>
  </div>;
}

function Notebook() {
  const [cells, setCells] = useState(() => {
    try { return JSON.parse(localStorage.getItem("sbase.notebook.cells") || "null") || [{ id: 1, type: "sql", source: "SELECT current_timestamp() AS now;", output: null }, { id: 2, type: "markdown", source: "# Lakehouse analysis\nDescribe your analysis here.", output: null }]; }
    catch { return [{ id: 1, type: "sql", source: "SELECT current_timestamp() AS now;", output: null }]; }
  });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => { localStorage.setItem("sbase.notebook.cells", JSON.stringify(cells)); }, [cells]);
  function update(id, patch) { setCells(current => current.map(c => c.id === id ? { ...c, ...patch } : c)); }
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
        setCells(current => current.map(c => c.id === cell.id ? { ...c, output: { ...output, ranAt: new Date().toISOString() } } : c));
      } catch (e) { setError("Cell " + cell.id + ": " + e.message); setCells(current => current.map(c => c.id === cell.id ? { ...c, output: { error: e.message } } : c)); break; }
    }
    setBusy(false);
  }
  function move(index, delta) {
    const target = index + delta; if (target < 0 || target >= cells.length) return;
    const copy = [...cells]; [copy[index], copy[target]] = [copy[target], copy[index]]; setCells(copy);
  }
  function add(type) { setCells(current => [...current, { id: Date.now() + Math.random(), type, source: type === "sql" ? "SELECT * FROM sbase.default.sample_table LIMIT 100;" : "## Notes\nWrite analysis notes here.", output: null }]); }
  return <div className="studio-panel notebook-panel">
    <div className="studio-heading"><div><div className="section-title">NOTEBOOK</div><h2>Lakehouse notebook</h2><p>Compose reproducible SQL analysis with explanatory Markdown cells.</p></div><div className="notebook-actions"><button onClick={() => add("markdown")}>+ Markdown</button><button onClick={() => add("sql")}>+ SQL cell</button><button className="studio-primary" onClick={runAll} disabled={busy}>{busy ? "Running…" : "▶ Run all"}</button></div></div>
    {error && <div className="studio-error">{error}</div>}
    {cells.map((cell, index) => <article className="notebook-cell" key={cell.id}><div className="cell-gutter"><b>{String(index + 1).padStart(2, "0")}</b><span>{cell.type.toUpperCase()}</span><button title="Move up" onClick={() => move(index, -1)} disabled={!index}>↑</button><button title="Move down" onClick={() => move(index, 1)} disabled={index === cells.length - 1}>↓</button><button title="Delete cell" onClick={() => setCells(current => current.filter(c => c.id !== cell.id))}>×</button></div><div className="cell-body"><textarea aria-label={cell.type + " cell"} spellCheck={cell.type === "markdown"} value={cell.source} onChange={e => update(cell.id, { source: e.target.value })}/>{cell.type === "sql" && <button className="cell-run" disabled={busy} onClick={() => execute(cell)}>▶ Run cell</button>}{cell.type === "markdown" && <pre className="markdown-preview">{cell.source}</pre>}{cell.output && (cell.output.error ? <div className="studio-error">{cell.output.error}</div> : <Results result={cell.output}/> )}</div></article>)}
    <div className="notebook-footer">Draft autosaved in this browser · {cells.length} cells · SQL executes on the SBase Spark engine</div>
  </div>;
}

export default function LakehouseStudio() {
  const [view, setView] = useState("lakehouse");
  return <section className="lakehouse-studio">
    <nav className="studio-nav"><div className="studio-brand"><span>◈</span> LAKEHOUSE STUDIO</div>{[["lakehouse","Catalog"],["sql","SQL Workspace"],["notebook","Notebook"]].map(([id, label]) => <button key={id} className={view === id ? "selected" : ""} onClick={() => setView(id)}>{label}</button>)}</nav>
    {view === "lakehouse" ? <LakehouseBrowser/> : view === "sql" ? <SqlWorkspace/> : <Notebook/>}
  </section>;
}
