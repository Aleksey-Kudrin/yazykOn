import { test, expect } from "@playwright/test";
const primary=process.env.SFU_PRIMARY_URL??"http://127.0.0.1:4100";
async function metrics(){const t=await (await fetch(primary+"/metrics")).text();const get=(n:string)=>Number((t.match(new RegExp("^"+n+"\\s+([0-9.eE+-]+)","m"))??[])[1]??0);return{peers:get("yazykon_media_active_peers"),rooms:get("yazykon_media_active_rooms"),tracks:get("yazykon_media_active_tracks")}}
test("media resource counters stay bounded during reconnect churn",async()=>{
 test.skip(!process.env.SFU_FAILOVER_LIVE,"Set SFU_FAILOVER_LIVE=1");const before=await metrics();const samples=[];for(let i=0;i<8;i++){await new Promise(r=>setTimeout(r,500));samples.push(await metrics())}
 expect(Math.max(...samples.map(x=>x.peers))-before.peers).toBeLessThanOrEqual(Number(process.env.SFU_RESOURCE_MAX_PEER_DELTA??12));
 expect(Math.max(...samples.map(x=>x.rooms))-before.rooms).toBeLessThanOrEqual(Number(process.env.SFU_RESOURCE_MAX_ROOM_DELTA??4));
 expect(Math.max(...samples.map(x=>x.tracks))-before.tracks).toBeLessThanOrEqual(Number(process.env.SFU_RESOURCE_MAX_TRACK_DELTA??12));
});
