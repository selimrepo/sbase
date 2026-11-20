import { useCallback, useMemo, useState } from "react";
import { ReactFlow, Background, Controls, MiniMap, addEdge, useEdgesState, useNodesState } from "@xyflow/react";

const initialNodes = [
  { id: "source", position: { x: 80, y: 150 }, data: { label: "Dataset input", kind: "source", operator: "dataset-values", config: { values: [2, 4, 8] } }, type: "default" },
  { id: "mining", position: { x: 360, y: 150 }, data: { label: "Mahout vector mining", kind: "mining", operator: "mahout-vector-sum", config: {} }, type: "default" }
];
const initialEdges = [{ id: "e1", source: "source", target: "mining", animated: true }];

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [workflowName, setWorkflowName] = useState("Untitled mining workflow");
  const [jobId, setJobId] = useState("");
  const [status, setStatus] = useState("Idle");
  const [output, setOutput] = useState("");
  const [apiBase, setApiBase] = useState("/api/v1");
  const [flinkJarId, setFlinkJarId] = useState("");
  const onConnect = useCallback((connection) => setEdges((eds) => addEdge(connection, eds)), [setEdges]);
  const definition = useMemo(() => ({ nodes: nodes.map((node) => ({ id: node.id, operator: node.data.operator, config: node.data.config })), edges: edges.map((edge) => ({ source: edge.source, target: edge.target })) }), [nodes, edges]);

  async function runWorkflow() {
    setStatus("Saving workflow…"); setOutput("");
    try {
      const created = await fetch(apiBase + "/workflows", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name: workflowName, definition }) });
      if (!created.ok) throw new Error("Workflow creation failed: HTTP " + created.status);
      const workflow = await created.json();
      const submitted = await fetch(apiBase + "/workflows/" + workflow.workflowId + "/runs", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ idempotencyKey: crypto.randomUUID(), maxAttempts: 3 }) });
      if (!submitted.ok) throw new Error("Job submission failed: HTTP " + submitted.status);
      const job = await submitted.json(); setJobId(job.jobId); setStatus("Queued");
      poll(job.jobId);
    } catch (error) { setStatus("Error"); setOutput(error.message); }
  }

  async function poll(id) {
    for (let i = 0; i < 120; i++) {
      try {
        const response = await fetch(apiBase + "/jobs/" + id);
        if (!response.ok) throw new Error("Status endpoint returned HTTP " + response.status);
        const state = await response.json(); setStatus(state.status + " · attempt " + state.attempt);
        if (["SUCCEEDED", "FAILED", "CANCELLED"].includes(state.status)) { setOutput(state.result || state.error || JSON.stringify(state, null, 2)); return; }
      } catch (error) { setOutput(error.message); }
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
    setStatus("Polling timed out");
  }

  function addNode(operator, label, config) {
    const id = crypto.randomUUID().slice(0, 8);
    setNodes((current) => [...current, { id, position: { x: 180 + current.length * 35, y: 320 + (current.length % 3) * 40 }, data: { label, operator, config } }]);
  }

  return <main className="shell">
    <header><div><div className="eyebrow">DISTRIBUTED DATA SCIENCE PLATFORM</div><h1>SBase <span>Workflow Studio</span></h1></div><div className="runtime"><i /> Pekko cluster runtime</div></header>
    <section className="toolbar">
      <input aria-label="Workflow name" value={workflowName} onChange={(e) => setWorkflowName(e.target.value)} />
      <input aria-label="API endpoint" value={apiBase} onChange={(e) => setApiBase(e.target.value)} />
      <button onClick={() => addNode("dataset-values", "Dataset values", { values: [1, 2, 3] })}>+ Dataset</button>
      <button onClick={() => addNode("spark-sql", "Spark SQL", { query: "SELECT 1 AS value" })}>+ Spark SQL</button>
      <button onClick={() => addNode("mahout-vector-sum", "Mahout vector sum", { values: [1, 2, 3] })}>+ Mahout sum</button>
      <button onClick={() => addNode("mahout-cosine-similarity", "Cosine similarity", { left: [1, 0], right: [1, 1] })}>+ Cosine</button>
      <button onClick={() => addNode("mahout-euclidean-distance", "Euclidean distance", { left: [0, 0], right: [3, 4] })}>+ Distance</button>
      <button onClick={() => addNode("mahout-dot-product", "Mahout dot product", { left: [1, 2, 3], right: [4, 5, 6] })}>+ Dot product</button>
      <button onClick={() => addNode("mahout-manhattan-distance", "Manhattan distance", { left: [1, 2], right: [4, 6] })}>+ Manhattan</button>
      <button onClick={() => addNode("mahout-normalize", "L2 normalize", { values: [3, 4] })}>+ Normalize</button>
      <button onClick={() => addNode("mahout-top-k", "Top-K coordinates", { values: [3, 9, 9, 1], k: 2 })}>+ Top-K</button>
      <button onClick={() => addNode("dataset-rows", "Tabular dataset", { rows: [{ region: "EU", amount: 10 }, { region: "US", amount: 20 }] })}>+ Table</button>
      <input aria-label="Flink uploaded JAR ID" placeholder="Flink JAR ID" value={flinkJarId} onChange={(e) => setFlinkJarId(e.target.value)} />
      <button onClick={() => addNode("flink-submit", "Flink stream job", { jarId: flinkJarId, parallelism: 1 })} disabled={!flinkJarId.trim()}>+ Flink</button>
      <button className="primary" onClick={runWorkflow}>Run workflow ↗</button>
    </section>
    <section className="workspace">
      <aside><div className="section-title">NODE CATALOG</div><p>Drag-and-connect processing stages in the canvas.</p><div className="catalog-item"><b>▦ Dataset</b><small>Input and data sources</small></div><div className="catalog-item"><b>⌘ Spark SQL</b><small>Distributed batch analytics</small></div><div className="catalog-item"><b>◇ Mahout</b><small>Vector and mining operators</small></div><div className="catalog-item"><b>⇢ Flink</b><small>Streaming job control</small></div><div className="section-title outputs-title">EXECUTION</div><div className="status">{status}</div>{jobId && <small className="job-id">Job {jobId}</small>}</aside>
      <div className="canvas"><div className="canvas-label">WORKFLOW GRAPH <span>{nodes.length} nodes · {edges.length} connections</span></div><ReactFlow nodes={nodes} edges={edges} onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} onConnect={onConnect} fitView><Background color="#283344" gap={22} /><MiniMap /><Controls /></ReactFlow></div>
    </section>
    <section className="results"><div><div className="section-title">RUN OUTPUT / DIAGNOSTICS</div><pre>{output || "Execution output and error details will appear here."}</pre></div><div><div className="section-title">WORKFLOW DEFINITION</div><pre>{JSON.stringify(definition, null, 2)}</pre></div></section>
    <footer>PostgreSQL durable queue · Pekko Cluster Sharding · Spark · Mahout · Flink REST</footer>
  </main>;
}
