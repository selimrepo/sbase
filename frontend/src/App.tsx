import {useEffect,useMemo,useState} from 'react'
import {NavLink,Route,Routes,useLocation} from 'react-router-dom'
import {Activity,Bell,BarChart3,BrainCircuit,ChevronDown,Clock3,Database,FlaskConical,Gauge,LayoutDashboard,Menu,Network,Search,Settings2,SlidersHorizontal,Sparkles,Workflow,X} from 'lucide-react'
import {useQuery} from '@tanstack/react-query'
import {get} from './api'
import {Home,Datasets,Pipelines,Mining,Features,Schedules,System} from './pages'
import AdvancedStudio from './AdvancedStudio'
import {MLStudio,BiStudio,FeatureParity} from './IntelligenceStudio'

type NavItem=readonly [string,string,typeof LayoutDashboard]
const groups:{name:string;items:NavItem[]}[]=[
 {name:'WORKSPACE',items:[['/','Overview',LayoutDashboard],['/datasets','Data catalog',Database],['/pipelines','Pipeline studio',Workflow],['/advanced','Advanced studio',SlidersHorizontal]]},
 {name:'INTELLIGENCE',items:[['/mining','Mining jobs',BrainCircuit],['/experiments','ML studio',FlaskConical],['/features','Feature parity',Database],['/feature-catalog','Feature catalog',Database],['/bi','BI & OLAP studio',BarChart3]]},
 {name:'OPERATIONS',items:[['/schedules','Schedules',Clock3],['/system','System health',Gauge]]}
]
const titles:Record<string,string>={'/':'Overview','/datasets':'Data catalog','/pipelines':'Pipeline studio','/advanced':'Advanced studio','/mining':'Mining jobs','/experiments':'ML studio','/features':'Feature parity','/feature-catalog':'Feature catalog','/bi':'BI & OLAP studio','/schedules':'Schedules','/system':'System health'}

export default function App(){
 const loc=useLocation()
 const [mobile,setMobile]=useState(false),[tokenOpen,setTokenOpen]=useState(false),[token,setToken]=useState(localStorage.getItem('sbase.accessToken')||''),[searchOpen,setSearchOpen]=useState(false),[searchQuery,setSearchQuery]=useState('')
 const health=useQuery({queryKey:['health'],queryFn:()=>get<{status:string}>('/actuator/health'),refetchInterval:30000,retry:0})
 const navigation=useMemo(()=>groups.flatMap(g=>g.items).filter(([,name])=>name.toLowerCase().includes(searchQuery.trim().toLowerCase())),[searchQuery])
 useEffect(()=>{setMobile(false);setSearchOpen(false);setSearchQuery('')},[loc.pathname])
 useEffect(()=>{
  const h=(e:KeyboardEvent)=>{
   if((e.metaKey||e.ctrlKey)&&e.key.toLowerCase()==='k'){e.preventDefault();setSearchQuery('');setSearchOpen(true)}
   if(e.key==='Escape'){setSearchOpen(false);setTokenOpen(false)}
  }
  window.addEventListener('keydown',h)
  return()=>window.removeEventListener('keydown',h)
 },[])
 const save=()=>{if(token.trim())localStorage.setItem('sbase.accessToken',token.trim());else localStorage.removeItem('sbase.accessToken');setTokenOpen(false);window.location.reload()}
 return <div className="shell">
  {mobile&&<button className="scrim" aria-label="Close navigation" onClick={()=>setMobile(false)}/>}
  <aside className={'sidebar '+(mobile?'open':'')}>
   <div className="brand"><div className="brandmark"><Network size={21}/></div><div><b>sbase<span>.</span></b><small>DATA INTELLIGENCE</small></div><button className="icon mobileclose" aria-label="Close menu" onClick={()=>setMobile(false)}><X/></button></div>
   <div className="workspace"><div className="workspace-logo">S</div><div><b>Personal workspace</b><small>Self-hosted environment</small></div><ChevronDown size={15}/></div>
   <nav aria-label="Main navigation">{groups.map(g=><section key={g.name}><label>{g.name}</label>{g.items.map(([to,name,Icon])=><NavLink end={to==='/'} key={to} to={to} className={({isActive})=>'navlink '+(isActive?'active':'')}><Icon size={17}/><span>{name}</span></NavLink>)}</section>)}</nav>
   <div className="sidebarbottom">
    <div className="snapshot"><div><Sparkles size={14}/> PLATFORM SNAPSHOT</div><b>One data workspace</b><small>Catalog · workflows · intelligence</small><div className="progress"><i/></div></div>
    <button className="navlink" onClick={()=>setTokenOpen(true)}><Settings2 size={17}/> API credentials</button>
    <div className="profile"><div className="avatar">SB</div><div><b>SBase Operator</b><small>{token?'Token configured':'Authentication required'}</small></div><button className="icon" aria-label="Open API credentials" onClick={()=>setTokenOpen(true)}><Settings2 size={16}/></button></div>
   </div>
  </aside>
  <main className="main">
   <header className="topbar">
    <div className="crumb"><button className="icon mobilemenu" aria-label="Open navigation" onClick={()=>setMobile(true)}><Menu/></button><span>Workspace</span><span className="crumb-separator">/</span><b>{titles[loc.pathname]||'Workspace'}</b></div>
    <div className="topright"><button className="search" onClick={()=>{setSearchQuery('');setSearchOpen(true)}}><Search size={15}/><span>Search workspace</span><kbd>⌘ K</kbd></button><NavLink className="connection-link" to="/system"><span className={'connection '+(health.isSuccess?'good':'')}><i/>{health.isSuccess?'API connected':'API status unknown'}</span></NavLink><button className="avatar" aria-label="API credentials" onClick={()=>setTokenOpen(true)}>SB</button></div>
   </header>
   <div className="content"><Routes><Route path="/" element={<Home health={health.isSuccess}/>}/><Route path="/datasets" element={<Datasets/>}/><Route path="/pipelines" element={<Pipelines/>}/><Route path="/advanced" element={<AdvancedStudio/>}/><Route path="/mining" element={<Mining/>}/><Route path="/experiments" element={<MLStudio/>}/><Route path="/features" element={<FeatureParity/>}/><Route path="/feature-catalog" element={<Features/>}/><Route path="/bi" element={<BiStudio/>}/><Route path="/schedules" element={<Schedules/>}/><Route path="/system" element={<System/>}/></Routes></div>
   <footer><span>© 2026 SBase Data Intelligence</span><span><i/> API status refreshed every 30 seconds</span></footer>
  </main>
  {tokenOpen&&<div className="overlay" onClick={()=>setTokenOpen(false)}><div className="modal" role="dialog" aria-modal="true" aria-labelledby="credentials-title" onClick={e=>e.stopPropagation()}><div className="modalhead"><div><label>SECURITY</label><h2 id="credentials-title">API authentication</h2></div><button className="icon" aria-label="Close dialog" onClick={()=>setTokenOpen(false)}><X/></button></div><p>Paste a JWT access token from your configured identity provider. The token is stored in this browser only.</p><label className="fieldlabel" htmlFor="access-token">Bearer token</label><textarea id="access-token" className="input code" rows={5} value={token} onChange={e=>setToken(e.target.value)} placeholder="eyJhbGciOi..."/><div className="modalactions"><button className="btn ghost" onClick={()=>{setToken('');localStorage.removeItem('sbase.accessToken');setTokenOpen(false);window.location.reload()}}>Clear token</button><button className="btn primary" onClick={save}>Save token</button></div></div></div>}
  {searchOpen&&<div className="overlay command-overlay" onClick={()=>setSearchOpen(false)}><div className="searchmodal" role="dialog" aria-modal="true" aria-label="Workspace search" onClick={e=>e.stopPropagation()}><div className="searchline"><Search/><input autoFocus value={searchQuery} aria-label="Search workspace sections" placeholder="Find a workspace section..." onChange={e=>setSearchQuery(e.target.value)}/><kbd>ESC</kbd></div><div className="searchitems">{navigation.length?navigation.map(([to,name,Icon])=><NavLink to={to} key={to} onClick={()=>setSearchOpen(false)}><Icon size={16}/>{name}<span>↵</span></NavLink>):<div className="search-empty">No matching workspace sections</div>}</div><div className="search-hint"><span>Navigate to a workspace</span><span><kbd>CTRL</kbd> + <kbd>K</kbd> to open</span></div></div></div>}
 </div>
}
