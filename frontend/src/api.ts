import axios from 'axios'
export const api=axios.create({baseURL:'/',timeout:20000,headers:{'Content-Type':'application/json'}})
api.interceptors.request.use(c=>{const t=localStorage.getItem('sbase.accessToken');if(t)c.headers.Authorization='Bearer '+t;return c})
export const get=async <T,>(url:string)=>(await api.get<T>(url)).data
export const post=async <T,>(url:string,body:unknown)=>(await api.post<T>(url,body)).data
export const errorText=(e:unknown)=>axios.isAxiosError(e)?((e.response?.data as {message?:string;detail?:string}|undefined)?.detail||(e.response?.data as {message?:string}|undefined)?.message||e.message):e instanceof Error?e.message:'Unexpected error'
export const date=(v?:string)=>v?new Intl.DateTimeFormat(undefined,{dateStyle:'medium',timeStyle:'short'}).format(new Date(v)):'—'
export type Dataset={id:string;name:string;namespace:string;tableName:string;description?:string;sourceType:string;createdAt:string}
export type Pipeline={id:string;name:string;description?:string;version:number;enabled:boolean;updatedAt:string}
export type Job={id:string;operation:string;sourceTable:string;outputTable:string;status:string;submittedAt:string;resultSummary?:string;error?:string}
