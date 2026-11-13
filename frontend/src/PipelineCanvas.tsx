import {useMemo,useState} from 'react'
import {useMutation,useQuery,useQueryClient} from '@tanstack/react-query'
import {Activity,AlertTriangle,ArrowRight,CheckCircle2,Clock3,Database,GitBranch,Play,Plus,RefreshCw,Save,Workflow,X} from 'lucide-react'
import {get,post,date,type Pipeline} from './api'

type Step=Record<string,any>&{id:string;type:string;dependsOn?:string[]}
type Run=Record<string,any>&{id:string;status:string}
const input='input'
const starter:Step[]=[
 {id:'source',type:'doris_read',table:'analytics.events',dependsOn:[]},
 {id:'output',type:'doris_write',table:'analytics.events_clean',mode:'append',idempotencyColumns:['id'],dependsOn:['source']}
]
function validate(steps:Step[]){
 const errors:string[]=[]
 const ids=new Set<string>()
 for(const s of steps){if(!s.id?.trim())errors.push('Every node needs an identifier.');else if(ids.has(s.id))errors.push('Duplicate node identifier: '+s.id);ids.add(s.id)}
 for(const s of steps)for(const dep of s.dependsOn||[])if(!steps.some(x=>x.id===dep))errors.push(s.id+' depends on missing node '+dep)
 const visiting=new Set<string>(),visited=new Set<string>()
 const byId=new Map(steps.map(s=>[s.id,s]))
 const visit=(id:string)=>{if(visiting.has(id)){errors.push('Dependency cycle detected at '+id);return}if(visited.has(id))return;visiting.add(id);for(const dep of byId.get(id)?.dependsOn||[])visit(dep);visiting.delete(id);visited.add(id)}
 for(const s of steps)visit(s.id)
 if(!steps.length)errors.push('Add at least one node.')
 return [...new Set(errors)]
}
function Panel({title,sub,children,extra}:{title:string;sub:string;children:React.ReactNode;extra?:React.ReactNode}){return <section className="panel"><div className="panelhead"><div><h2>{title}</h2><p>{sub}</p></div>{extra}</div>{children}</section>}
function Field({label,children}:{label:string;children:React.ReactNode}){return <label className="field"><span>{label}</span>{children}</label>}
function ErrorView({error}:{error:unknown}){return <div className="error">{error instanceof Error?error.message:String(error)}</div>}
export default function PipelineCanvas(){
 const qc=useQueryClient()
 const pipelines=useQuery({queryKey:['pipelines'],queryFn:()=>get<Pipeline[]>('/api/pipelines')})
 const [name,setName]=useState('Customer events pipeline'),[description,setDescription]=useState('Ingest, transform and publish customer events'),[enabled,setEnabled]=useState(true)
 const [steps,setSteps]=useState<Step[]>(starter),[selected,setSelected]=useState('source'),[selectedPipeline,setSelectedPipeline]=useState('')
 const [showEditor,setShowEditor]=useState(true)
 const [runsOpen,setRunsOpen]=useState(false)
 const errors=useMemo(()=>validate(steps),[steps])
 const active=steps.find(s=>s.id===selected)
 const runs=useQuery({queryKey:['pipeline-runs',selectedPipeline],queryFn:()=>get<Run[]>('/api/pipelines/'+selectedPipeline+'/runs'),enabled:!!selectedPipeline,refetchInterval:3000})
 const create=useMutation({mutationFn:()=>post('/api/pipelines',{name,description,definitionJson:JSON.stringify({steps},null,2),enabled}),onSuccess:()=>{qc.invalidateQueries({queryKey:['pipelines']});setShowEditor(false)}})
 const run=useMutation({mutationFn:(id:string)=>post('/api/pipelines/'+id+'/runs',{}),onSuccess:(_r,id)=>{setSelectedPipeline(id);setRunsOpen(true);qc.invalidateQueries({queryKey:['pipeline-runs',id]})}})
 const updateStep=(next:Step)=>setSteps(current=>current.map(s=>s.id===next.id?next:s))
 const addStep=(type:'doris_read'|'doris_write')=>{
  const id=(type==='doris_read'?'source':'output')+'_'+(steps.length+1)
  const previous=steps.length?steps[steps.length-1].id:undefined
  const next:Step=type==='doris_read'?{id,type,table:'analytics.source_table',dependsOn:previous?[previous]:[]}:{id,type,table:'analytics.output_table',mode:'append',idempotencyColumns:['id'],dependsOn:previous?[previous]:[]}
  setSteps(current=>[...current,next]);setSelected(id)
 }
 const removeStep=()=>{if(!active)return;setSteps(current=>current.filter(s=>s.id!==active.id).map(s=>({...s,dependsOn:(s.dependsOn||[]).filter(d=>d!==active.id)})));setSelected(steps.find(s=>s.id!==active.id)?.id||'')}
 const toggleDependency=(dep:string,checked:boolean)=>{if(!active)return;const deps=active.dependsOn||[];updateStep({...active,dependsOn:checked?[...deps,dep]:deps.filter(x=>x!==dep)})}
 const graphWidth=Math.max(steps.length*250+80,680)
 const positions=new Map(steps.map((s,i)=>[s.id,{x:40+i*250,y:95}]))
 return <div className="pipeline-studio">
  <div className="pagehead"><div><label>WORKSPACE / ORCHESTRATION</label><h1>Pipeline studio</h1><p>Compose dependency-aware Doris workflows, validate the DAG and launch persisted executions.</p></div><button className="btn primary" onClick={()=>setShowEditor(v=>!v)}><Plus size={15}/>{showEditor?'Close editor':'New pipeline'}</button></div>
  <div className="metrics three"><div className="metric"><div className="metricrow">Registered pipelines <i><Workflow size={17}/></i></div><b>{pipelines.data?.length||0}</b><small>Saved workflow definitions</small></div><div className="metric"><div className="metricrow">Selected nodes <i className="blue"><GitBranch size={17}/></i></div><b>{steps.length}</b><small>In current visual definition</small></div><div className="metric"><div className="metricrow">Validation <i className="green"><CheckCircle2 size={17}/></i></div><b>{errors.length?'Invalid':'Ready'}</b><small>{errors.length?errors.length+' issue(s)':'No dependency errors detected'}</small></div></div>
  {showEditor&&<Panel title="Visual DAG editor" sub="Select a node to configure it. Add source and sink nodes, then define dependency edges." extra={<span className="hint">{steps.length} nodes · {steps.reduce((n,s)=>n+(s.dependsOn||[]).length,0)} edges</span>}>
   <div className="pipeline-toolbar"><div className="pipeline-palette"><button className="btn secondary" onClick={()=>addStep('doris_read')}><Plus size={14}/><Database size={14}/> Add Doris source</button><button className="btn secondary" onClick={()=>addStep('doris_write')}><Plus size={14}/><Workflow size={14}/> Add Doris sink</button></div><div className="pipeline-toolbar-actions"><button className="btn ghost" onClick={()=>{setSteps(starter);setSelected('source')}}>Reset example</button><button className="btn primary" disabled={!!errors.length||create.isPending||!name.trim()} onClick={()=>create.mutate()}><Save size={14}/> Save pipeline</button></div></div>
   <div className="pipeline-canvas-wrap"><div className="pipeline-canvas" style={{minWidth:graphWidth}}>
    <svg className="pipeline-edges" width={graphWidth} height="230" aria-label="Pipeline dependency edges">
     <defs><marker id="pipeline-arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L0,6 L6,3 z" fill="#8d83ed"/></marker></defs>
     {steps.flatMap(s=>(s.dependsOn||[]).map(dep=>{const a=positions.get(dep),b=positions.get(s.id);return a&&b?<path key={dep+'-'+s.id} d={`M ${a.x+178} ${a.y+48} C ${a.x+215} ${a.y+48}, ${b.x-40} ${b.y+48}, ${b.x} ${b.y+48}`} fill="none" stroke="#8d83ed" strokeWidth="2" markerEnd="url(#pipeline-arrow)"/>:null}))}
    </svg>
    {steps.map((s,i)=>{const p=positions.get(s.id)!;return <button key={s.id} className={'pipeline-node '+(selected===s.id?'selected':'')} style={{left:p.x,top:p.y}} onClick={()=>setSelected(s.id)}><span className="pipeline-node-icon">{s.type==='doris_read'?<Database size={17}/>:<Workflow size={17}/>}</span><span className="pipeline-node-copy"><b>{s.id}</b><small>{s.type}</small><em>{s.table||'Configure table'}</em></span><span className="pipeline-node-index">{String(i+1).padStart(2,'0')}</span></button>})}
    {!steps.length&&<div className="pipeline-canvas-empty">Add a Doris source node to start the workflow.</div>}
   </div></div>
   <div className="pipeline-editor-grid">
    <section className="pipeline-inspector"><div className="panelhead"><div><h2>Node configuration</h2><p>{active?active.id:'Select a node on the canvas'}</p></div>{active&&<button className="icon" aria-label="Remove selected node" onClick={removeStep}><X size={16}/></button>}</div>
     {active?<><Field label="Node identifier"><input className={input} value={active.id} onChange={e=>updateStep({...active,id:e.target.value})}/></Field><Field label="Node type"><select className={input} value={active.type} onChange={e=>updateStep({...active,type:e.target.value})}><option value="doris_read">Doris read</option><option value="doris_write">Doris write</option></select></Field><Field label="Target table"><input className={input} value={active.table||''} onChange={e=>updateStep({...active,table:e.target.value})} placeholder="analytics.table_name"/></Field>
      {active.type==='doris_write'&&<><Field label="Write mode"><select className={input} value={active.mode||'append'} onChange={e=>updateStep({...active,mode:e.target.value})}><option value="append">Append</option><option value="overwrite">Overwrite</option><option value="upsert">Upsert</option></select></Field><Field label="Idempotency columns (comma separated)"><input className={input} value={(active.idempotencyColumns||[]).join(',')} onChange={e=>updateStep({...active,idempotencyColumns:e.target.value.split(',').map(x=>x.trim()).filter(Boolean)})}/></Field></>}
      <div className="field"><span>Depends on</span><div className="pipeline-dependencies">{steps.filter(s=>s.id!==active.id).map(s=><label key={s.id}><input type="checkbox" checked={(active.dependsOn||[]).includes(s.id)} onChange={e=>toggleDependency(s.id,e.target.checked)}/>{s.id}</label>)}</div></div>
     </>:<p className="hint">Choose a node to inspect and edit its configuration.</p>}
    </section>
    <section className="pipeline-json"><div className="panelhead"><div><h2>Definition preview</h2><p>Exact JSON payload submitted to the existing pipeline API</p></div></div><pre>{JSON.stringify({steps},null,2)}</pre></section>
   </div>
   <div className="pipeline-meta"><Field label="Pipeline name"><input className={input} value={name} onChange={e=>setName(e.target.value)}/></Field><Field label="Description"><input className={input} value={description} onChange={e=>setDescription(e.target.value)}/></Field><label className="check"><input type="checkbox" checked={enabled} onChange={e=>setEnabled(e.target.checked)}/> Enable definition</label></div>
   {errors.length>0&&<div className="pipeline-validation invalid"><AlertTriangle size={16}/><div><b>Validation needs attention</b>{errors.map(e=><span key={e}>{e}</span>)}</div></div>}
   {errors.length===0&&<div className="pipeline-validation valid"><CheckCircle2 size={16}/> DAG structure and dependencies are valid.</div>}
   {create.isError&&<ErrorView error={create.error}/>}
  </Panel>}
  <Panel title="Pipeline registry" sub="Saved workflows and direct execution controls" extra={<button className="icon" onClick={()=>pipelines.refetch()}><RefreshCw size={16}/></button>}>
   {pipelines.isLoading?<div className="loading">Loading pipelines…</div>:pipelines.isError?<ErrorView error={pipelines.error}/>:pipelines.data?.length?<div className="tablewrap"><table><thead><tr><th>Pipeline</th><th>Version</th><th>State</th><th>Updated</th><th>Actions</th></tr></thead><tbody>{pipelines.data.map(p=><tr key={p.id}><td><b>{p.name}</b><small>{p.description||'No description'}</small></td><td>v{p.version}</td><td><em className={p.enabled?'good':''}>{p.enabled?'Enabled':'Disabled'}</em></td><td>{date(p.updatedAt)}</td><td><button className="btn secondary" disabled={!p.enabled||run.isPending} onClick={()=>run.mutate(p.id)}><Play size={13}/> Run</button> <button className="btn ghost" onClick={()=>{setSelectedPipeline(p.id);setRunsOpen(true)}}>History</button></td></tr>)}</tbody></table></div>:<div className="empty"><b>No pipelines yet</b><p>Create a DAG using the visual editor above.</p></div>}
   {run.isError&&<ErrorView error={run.error}/>}
  </Panel>
  {runsOpen&&selectedPipeline&&<Panel title="Execution monitor" sub="Persisted pipeline runs; active executions refresh automatically" extra={<button className="icon" onClick={()=>runs.refetch()}><RefreshCw size={16}/></button>}>
   {runs.isLoading?<div className="loading">Loading execution history…</div>:runs.isError?<ErrorView error={runs.error}/>:runs.data?.length?<div className="tablewrap"><table><thead><tr><th>Run</th><th>Status</th><th>Version</th><th>Records</th><th>Submitted</th><th>Error</th></tr></thead><tbody>{runs.data.map(r=><tr key={r.id}><td><code>{r.id.slice(0,8)}…</code></td><td><em className={(r.status||'').toLowerCase()}>{r.status}</em></td><td>v{r.pipelineVersion??'—'}</td><td>{Number(r.recordsProcessed||0).toLocaleString()}</td><td>{date(r.submittedAt)}</td><td>{r.errorMessage||'—'}</td></tr>)}</tbody></table></div>:<div className="empty"><b>No executions yet</b><p>Run the workflow to create an execution record.</p></div>}
  </Panel>}
 </div>
}
