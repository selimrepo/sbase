import {useEffect,useMemo,useState} from 'react'
import {useMutation,useQuery,useQueryClient} from '@tanstack/react-query'
import {api,get,errorText,date} from './api'
import {Activity,ArrowDownRight,Boxes,Database,GitBranch,Layers3,RefreshCw,Save,ShieldCheck,Tag} from 'lucide-react'
import './CatalogLineageStudio.css'

type Asset={
 id:string;assetType:string;externalKey:string;displayName:string;namespace?:string;physicalName?:string
 description?:string;ownerSubject:string;ownerTeam?:string;domain?:string;classification:string
 tags:string[];metadata:Record<string,unknown>;parentId?:string;lifecycleState:string
 schemaFingerprint?:string;createdAt:string;updatedAt:string;ordinalPosition?:number;dataType?:string;nullable?:boolean
}
type Edge={id:string;fromAssetId:string;toAssetId:string;edgeType:string;evidenceType:string;evidenceRef?:string
 expressionText?:string;confidence:number;observedAt:string;effectiveVersion:number;runId?:string}
type Graph={rootId:string;direction:string;depth:number;truncated:boolean;nodes:Asset[];edges:Edge[]}
const types=['ALL','DATA_SOURCE','CONNECTOR','TABLE','COLUMN','PIPELINE','PIPELINE_STEP','FEATURE_SET','MODEL','REPORT','REPORT_FIELD','DASHBOARD','WIDGET']
const classification=['PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED']
const title=(v:string)=>v.toLowerCase().split('_').map(x=>x.charAt(0).toUpperCase()+x.slice(1)).join(' ')
const short=(v?:string)=>v?v.length>14?v.slice(0,8)+'…'+v.slice(-4):v:'—'
function Panel({title:heading,sub,children,extra}:{title:string;sub?:string;children:React.ReactNode;extra?:React.ReactNode}){return <section className="panel"><div className="panelhead"><div><h2>{heading}</h2>{sub&&<p>{sub}</p>}</div>{extra}</div>{children}</section>}
function Empty({children}:{children:React.ReactNode}){return <div className="empty"><div><GitBranch/></div><b>{children}</b></div>}
export default function CatalogLineageStudio(){
 const qc=useQueryClient()
 const [type,setType]=useState('ALL'),[search,setSearch]=useState(''),[rootId,setRootId]=useState(''),[mode,setMode]=useState<'graph'|'impact'>('graph')
 const [ownerTeam,setOwnerTeam]=useState(''),[ownerSubject,setOwnerSubject]=useState(''),[domain,setDomain]=useState(''),[classificationValue,setClassificationValue]=useState('INTERNAL'),[tags,setTags]=useState(''),[description,setDescription]=useState('')
 const assets=useQuery({queryKey:['catalog-assets',type],queryFn:()=>api.get<Asset[]>('/api/catalog/assets',{params:{type:type==='ALL'?undefined:type,limit:200}}).then(r=>r.data),staleTime:15000})
 const filtered=useMemo(()=> (assets.data||[]).filter(a=>[a.displayName,a.externalKey,a.namespace||'',a.ownerSubject,a.ownerTeam||'',a.domain||''].join(' ').toLowerCase().includes(search.toLowerCase())),[assets.data,search])
 useEffect(()=>{if(filtered.length&&!filtered.some(a=>a.id===rootId))setRootId(filtered[0].id);else if(!filtered.length)setRootId('')},[filtered,rootId])
 const selected=assets.data?.find(a=>a.id===rootId)
 useEffect(()=>{if(selected){setOwnerTeam(selected.ownerTeam||'');setOwnerSubject(selected.ownerSubject||'');setDomain(selected.domain||'');setClassificationValue(selected.classification);setTags((selected.tags||[]).join(', '));setDescription(selected.description||'')}},[rootId,selected?.updatedAt])
 const graph=useQuery({queryKey:['catalog-lineage-graph',rootId,mode],queryFn:()=>get<Graph>(mode==='impact'?'/api/catalog/lineage/impact?assetId='+rootId+'&depth=8':'/api/catalog/lineage/graph?rootId='+rootId+'&direction=BOTH&depth=4'),enabled:!!rootId,staleTime:10000})
 const save=useMutation({mutationFn:()=>api.patch('/api/catalog/assets/'+rootId+'/governance',{description,ownerSubject,ownerTeam,domain,classification:classificationValue,tags:tags.split(',').map(x=>x.trim()).filter(Boolean)}),onSuccess:()=>{qc.invalidateQueries({queryKey:['catalog-assets']});qc.invalidateQueries({queryKey:['catalog-lineage-graph']})}})
 const nodeById=useMemo(()=>new Map((graph.data?.nodes||[]).map(n=>[n.id,n])),[graph.data])
 const childColumns=graph.data?.nodes.filter(n=>n.assetType==='COLUMN'&&n.parentId===selected?.id)||[]
 const choose=(id:string)=>{setRootId(id);setMode('graph')}
 return <div className="catalog-lineage-page">
  <div className="pagehead"><div><label>GOVERNANCE / METADATA GRAPH</label><h1>Catalog & lineage.</h1><p>Explore governed data assets, column metadata and end-to-end dependencies across pipelines, reports and dashboards.</p></div><button className="btn ghost" onClick={()=>{assets.refetch();graph.refetch()}}><RefreshCw size={15}/> Refresh</button></div>
  <div className="catalog-lineage-metrics">
   <div><Database/><span>Catalog assets</span><b>{assets.data?.length??'—'}</b></div>
   <div><GitBranch/><span>Graph nodes</span><b>{graph.data?.nodes.length??'—'}</b></div>
   <div><Activity/><span>Lineage edges</span><b>{graph.data?.edges.length??'—'}</b></div>
   <div><ShieldCheck/><span>Certified assets</span><b>{assets.data?.filter(a=>a.lifecycleState==='CERTIFIED').length??'—'}</b></div>
  </div>
  <div className="catalog-lineage-layout">
   <Panel title="Asset explorer" sub="Tenant-scoped catalog inventory">
    <div className="catalog-filters"><input className="input" placeholder="Search assets, owners, domains…" value={search} onChange={e=>setSearch(e.target.value)}/><select className="input" value={type} onChange={e=>setType(e.target.value)}>{types.map(t=><option key={t} value={t}>{t==='ALL'?'All asset types':title(t)}</option>)}</select></div>
    {assets.isLoading?<div className="loading">Loading catalog…</div>:assets.isError?<div className="error">{errorText(assets.error)}</div>:filtered.length===0?<Empty>No matching catalog assets</Empty>:<div className="catalog-asset-list">{filtered.map(a=><button key={a.id} className={'catalog-asset-row '+(rootId===a.id?'selected':'')} onClick={()=>choose(a.id)}><span className="catalog-asset-icon">{a.assetType==='TABLE'||a.assetType==='COLUMN'?<Database size={16}/>:a.assetType==='PIPELINE'||a.assetType==='PIPELINE_STEP'?<GitBranch size={16}/>:a.assetType==='DASHBOARD'||a.assetType==='WIDGET'?<Layers3 size={16}/>:<Boxes size={16}/>}</span><span className="catalog-asset-name"><b>{a.displayName}</b><small>{title(a.assetType)} · {a.namespace? a.namespace+'.':''}{a.physicalName||a.externalKey}</small></span><span className={'catalog-state '+a.lifecycleState.toLowerCase()}>{a.lifecycleState}</span></button>)}</div>}
   <div className="catalog-list-foot">{filtered.length} shown · maximum 200 per request</div>
   </Panel>
   <div className="catalog-detail-column">
    {selected?<>
     <Panel title={selected.displayName} sub={title(selected.assetType)+' · '+selected.externalKey} extra={<span className={'catalog-state '+selected.lifecycleState.toLowerCase()}>{selected.lifecycleState}</span>}>
      <div className="catalog-detail-grid"><div><small>Owner</small><b>{selected.ownerSubject}</b></div><div><small>Responsible team</small><b>{selected.ownerTeam||'Unassigned'}</b></div><div><small>Domain</small><b>{selected.domain||'Unassigned'}</b></div><div><small>Classification</small><b>{selected.classification}</b></div><div><small>Last metadata update</small><b>{date(selected.updatedAt)}</b></div><div><small>Schema fingerprint</small><b className="mono">{short(selected.schemaFingerprint)}</b></div></div>
      {selected.assetType==='COLUMN'&&<div className="catalog-column-summary"><b>Column contract</b><span>{selected.dataType||'Unknown type'} · {selected.nullable?'Nullable':'Not null'} · Position {selected.ordinalPosition??'—'}</span></div>}
      {selected.assetType==='TABLE'&&<div className="catalog-column-summary"><b>Physical relation</b><span>{selected.namespace}.{selected.physicalName}</span><small>Choose a column asset from the explorer to inspect its contract.</small></div>}
      <div className="catalog-tags">{(selected.tags||[]).map(t=><span key={t}><Tag size={12}/>{t}</span>)}</div>
      <div className="catalog-governance-form"><h3>Governance metadata</h3><label>Description<input className="input" value={description} onChange={e=>setDescription(e.target.value)} placeholder="Business meaning and intended use"/></label><div className="catalog-form-grid"><label>Data owner subject<input className="input" value={ownerSubject} onChange={e=>setOwnerSubject(e.target.value)}/></label><label>Responsible team<input className="input" value={ownerTeam} onChange={e=>setOwnerTeam(e.target.value)}/></label><label>Domain<input className="input" value={domain} onChange={e=>setDomain(e.target.value)}/></label><label>Classification<select className="input" value={classificationValue} onChange={e=>setClassificationValue(e.target.value)}>{classification.map(x=><option key={x}>{x}</option>)}</select></label></div><label>Tags (comma separated)<input className="input" value={tags} onChange={e=>setTags(e.target.value)} placeholder="finance, gold, pii"/></label><button className="btn primary" onClick={()=>save.mutate()} disabled={save.isPending}><Save size={15}/>{save.isPending?'Saving…':'Save governance'}</button>{save.isError&&<div className="error">{errorText(save.error)}</div>}</div>
     </Panel>
     {selected.assetType==='TABLE'&&<Panel title="Column dictionary" sub="First-class column contracts">
      {childColumns.length?childColumns.map(n=><button key={n.id} className="catalog-column-row" onClick={()=>choose(n.id)}><span><b>{n.displayName}</b><small>{n.description||'No business description'}</small></span><code>{n.dataType||'UNKNOWN'}</code><small>{n.nullable?'NULL':'NOT NULL'}</small></button>):<Empty>Column details appear when a table graph is loaded</Empty>}
     </Panel>}
    </>:<Panel title="Asset details" sub="Select a catalog asset"><Empty>No asset selected</Empty></Panel>}
    <Panel title={mode==='impact'?'Downstream impact':'Lineage explorer'} sub={mode==='impact'?'All reachable downstream consumers and dependencies':'Upstream and downstream graph, bounded to four hops'} extra={<div className="catalog-mode-switch"><button className={mode==='graph'?'active':''} onClick={()=>setMode('graph')}><GitBranch size={13}/> Graph</button><button className={mode==='impact'?'active':''} onClick={()=>setMode('impact')}><ArrowDownRight size={13}/> Impact</button></div>}>
     {graph.isLoading?<div className="loading">Resolving lineage…</div>:graph.isError?<div className="error">{errorText(graph.error)}</div>:!graph.data||graph.data.nodes.length<2?<Empty>No lineage edges have been registered for this asset yet</Empty>:<>
      <div className="lineage-summary"><span>{graph.data.nodes.length} nodes</span><span>{graph.data.edges.length} relationships</span>{graph.data.truncated&&<b>Result capped — refine the selected root</b>}</div>
      <div className="lineage-nodes">{graph.data.nodes.map(n=><button key={n.id} className={'lineage-node '+(n.id===rootId?'root':'')} onClick={()=>choose(n.id)}><span>{title(n.assetType)}</span><b>{n.displayName}</b><small>{n.ownerTeam||n.ownerSubject}</small></button>)}</div>
      <div className="lineage-edges">{graph.data.edges.map(e=><div key={e.id} className="lineage-edge"><span>{nodeById.get(e.fromAssetId)?.displayName||short(e.fromAssetId)}</span><i>{e.edgeType}</i><ArrowDownRight size={14}/><span>{nodeById.get(e.toAssetId)?.displayName||short(e.toAssetId)}</span><small>{e.evidenceType} · {(e.confidence*100).toFixed(0)}% · v{e.effectiveVersion}</small></div>)}</div>
     </>}
    </Panel>
   </div>
  </div>
 </div>
}
