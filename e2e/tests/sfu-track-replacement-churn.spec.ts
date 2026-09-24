import { test, expect, type Page } from "@playwright/test";
import { createHmac } from "node:crypto";
const primary=process.env.SFU_PRIMARY_URL??"http://127.0.0.1:4100", secret=process.env.ROOM_ACCESS_SECRET??"integration-secret";
const token=(r:string,u:string)=>{const p=Buffer.from(JSON.stringify({roomId:r,userId:u,role:"participant",exp:Math.floor(Date.now()/1000)+600})).toString("base64url");return p+"."+createHmac("sha256",secret).update(p).digest("base64url")};
async function join(page:Page,roomId:string,userId:string,oldPeer="",replace=false){return page.evaluate(async({roomId,userId,accessToken,oldPeer,replace})=>{
 const s:any=globalThis.__replace??={stream:undefined,pc:undefined,ws:undefined,old:[]};s.ws?.close();s.pc?.close();s.stream??=await navigator.mediaDevices.getUserMedia({video:true,audio:false});
 if(replace){const old=s.stream.getVideoTracks()[0];s.old.push(old.id);const fresh=await navigator.mediaDevices.getUserMedia({video:true,audio:false});s.stream.removeTrack(old);old.stop();s.stream.addTrack(fresh.getVideoTracks()[0])}
 const pc=new RTCPeerConnection();for(const t of s.stream.getTracks())pc.addTrack(t,s.stream);const ws=new WebSocket(primary.replace(/^http/,"ws")+"/ws");const q:any[]=[];const w=new Map<string,((x:any)=>void)[]>();
 const wait=(t:string)=>new Promise<any>((res,rej)=>{const tm=setTimeout(()=>rej(Error(t)),12000);const i=q.findIndex(x=>x.type===t);if(i>=0){clearTimeout(tm);return res(q.splice(i,1)[0])}const a=w.get(t)??[];a.push(x=>{clearTimeout(tm);res(x)});w.set(t,a)});
 ws.onmessage=e=>{const m=JSON.parse(e.data);if(m.type==="ice"&&m.data)void pc.addIceCandidate(m.data);const a=w.get(m.type);if(a?.length)a.shift()!(m);else q.push(m)};await new Promise<void>((res,rej)=>{ws.onopen=()=>res();ws.onerror=()=>rej(Error("ws"))});pc.onicecandidate=e=>{if(e.candidate&&ws.readyState===1)ws.send(JSON.stringify({type:"ice",roomId,data:e.candidate.toJSON()}))};
 ws.send(JSON.stringify({type:"join",roomId,data:{accessToken,reconnect:!!oldPeer,...oldPeer?{peerId:oldPeer}:{}}}));const j=await wait("joined");const o=await pc.createOffer();await pc.setLocalDescription(o);ws.send(JSON.stringify({type:"offer",roomId,data:pc.localDescription}));const a=await wait("answer");if(a.data)await pc.setRemoteDescription(a.data);s.pc=pc;s.ws=ws;return{peerId:j.peerId,trackIds:s.stream.getTracks().map((x:MediaStreamTrack)=>x.id),old:s.old.slice()};
 },{roomId,userId,accessToken:token(roomId,userId),oldPeer,replace})}
test("repeated media track replacement never reuses stopped track",async({browser})=>{
 test.skip(!process.env.SFU_FAILOVER_LIVE,"Set SFU_FAILOVER_LIVE=1");const room="TRACK-CHURN-"+Date.now();const p=await browser.newPage({permissions:["camera","microphone"]});try{
  let r=await join(p,room,"track-user");const peer=r.peerId;const ids=new Set(r.trackIds);
  for(let i=0;i<6;i++){r=await join(p,room,"track-user",peer,true);expect(r.peerId).toBe(peer);expect(r.trackIds.every((x:string)=>!ids.has(x))).toBeTruthy();r.trackIds.forEach((x:string)=>ids.add(x));expect(r.old.length).toBe(i+1)}
 }finally{await p.close()}
});