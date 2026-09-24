import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import { test, expect, type Page } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const compose = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const ttl = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const rooms = Math.min(4, Math.max(2, Number(process.env.SFU_ISOLATION_ROOMS ?? 3)));
const members = 3;

const token = (roomId:string,userId:string) => {
  const p=Buffer.from(JSON.stringify({roomId,userId,role:userId.endsWith("-0")?"host":"participant",exp:Math.floor(Date.now()/1000)+900})).toString("base64url");
  return p+"."+createHmac("sha256",secret).update(p).digest("base64url");
};
async function redis(roomIds:string[]) {
  const {createClient}=await import("redis"); const c=createClient({url:redisUrl}); await c.connect();
  const out:any[]=[];
  for(const roomId of roomIds){
    const o=await c.get("yazykon:sfu:owner:"+roomId);
    const peers=await c.keys("yazykon:sfu:peer:"+roomId+":*");
    out.push({roomId,owner:o?JSON.parse(o):null,peers:peers.sort(),state:await c.exists("yazykon:sfu:state:"+roomId),tracks:await c.exists("yazykon:sfu:tracks:"+roomId)});
  }
  await c.quit(); return out;
}
async function connect(page:Page,endpoint:string,roomId:string,userId:string,peerId=""){
  return page.evaluate(async({endpoint,roomId,accessToken,peerId})=>{
    const s:any=globalThis.__iso??={stream:undefined,ws:undefined,pc:undefined};
    s.ws?.close();s.pc?.close();s.stream??=await navigator.mediaDevices.getUserMedia({video:true,audio:false});
    const pc=new RTCPeerConnection(); for(const t of s.stream.getTracks()) pc.addTrack(t,s.stream);
    let remote=0; pc.ontrack=()=>remote++;
    const ws=new WebSocket(endpoint.replace(/^http/,"ws")+"/ws"); const q:any[]=[]; const waits=new Map<string,((x:any)=>void)[]>();
    const wait=(type:string)=>new Promise<any>((res,rej)=>{const tm=setTimeout(()=>rej(Error("timeout "+type)),12000);const i=q.findIndex(x=>x.type===type);if(i>=0){clearTimeout(tm);return res(q.splice(i,1)[0]);}const a=waits.get(type)??[];a.push(x=>{clearTimeout(tm);res(x)});waits.set(type,a)});
    ws.onmessage=e=>{const m=JSON.parse(e.data);if(m.type==="ice"&&m.data)void pc.addIceCandidate(m.data);const a=waits.get(m.type);if(a?.length)a.shift()!(m);else q.push(m)};
    await new Promise<void>((res,rej)=>{ws.onopen=()=>res();ws.onerror=()=>rej(Error("ws"))});
    pc.onicecandidate=e=>{if(e.candidate&&ws.readyState===1)ws.send(JSON.stringify({type:"ice",roomId,data:e.candidate.toJSON()}))};
    ws.send(JSON.stringify({type:"join",roomId,data:{accessToken,reconnect:!!peerId,...peerId?{peerId}:{}}}));
    const j=await wait("joined");const offer=await pc.createOffer();await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({type:"offer",roomId,data:pc.localDescription}));const a=await wait("answer");if(a.data)await pc.setRemoteDescription(a.data);
    s.ws=ws;s.pc=pc;return {peerId:j.peerId,trackIds:s.stream.getTracks().map((x:MediaStreamTrack)=>x.id),remote};
  },{endpoint,roomId,userId,accessToken:token(roomId,userId),peerId});
}
test("SFU rooms remain isolated during asymmetric churn",async({browser})=>{
  test.skip(!process.env.SFU_FAILOVER_LIVE,"Set SFU_FAILOVER_LIVE=1");
  const ids=Array.from({length:rooms},(_,i)=>"ISO-"+Date.now()+"-"+i);const pages:Page[]=[];const people:any[]=[];
  try{
    for(const roomId of ids)for(let i=0;i<members;i++){const page=await browser.newPage({permissions:["camera","microphone"]});pages.push(page);people.push({page,roomId,userId:roomId+"-u-"+i,peerId:"",tracks:[]})}
    const first=await Promise.all(people.map(p=>connect(p.page,primary,p.roomId,p.userId)));
    first.forEach((r,i)=>{people[i].peerId=r.peerId;people[i].tracks=r.trackIds});
    const base=await redis(ids);expect(base.every(x=>x.peers.length===members)).toBeTruthy();
    execFileSync("docker",["compose","-f",compose,"stop","sfu-primary"],{stdio:"inherit"});await new Promise(r=>setTimeout(r,ttl+700));
    const takeover=await redis(ids);expect(takeover.every(x=>x.owner?.nodeId==="integration-secondary")).toBeTruthy();
    const subset=people.filter((_,i)=>i%2===0);
    const rec=await Promise.all(subset.map(p=>connect(p.page,secondary,p.roomId,p.userId,p.peerId)));
    rec.forEach((r,i)=>expect(r.peerId).toBe(subset[i].peerId));
    const mid=await redis(ids);
    for(const x of mid)expect(x.peers.length).toBeGreaterThanOrEqual(1);
    execFileSync("docker",["compose","-f",compose,"start","sfu-primary"],{stdio:"inherit"});await new Promise(r=>setTimeout(r,500));
    const all=await Promise.all(people.map(p=>connect(p.page,secondary,p.roomId,p.userId,p.peerId)));
    all.forEach((r,i)=>expect(r.peerId).toBe(people[i].peerId));
    const final=await redis(ids);expect(final.every(x=>x.peers.length===members)).toBeTruthy();
    for(const x of final){const foreign=final.filter(y=>y.roomId!==x.roomId).flatMap(y=>y.peers);expect(x.peers.some(k=>foreign.includes(k))).toBeFalsy()}
  }finally{try{execFileSync("docker",["compose","-f",compose,"start","redis","sfu-primary","sfu-secondary"],{stdio:"inherit"})}catch{}await Promise.all(pages.map(p=>p.close()))}
});