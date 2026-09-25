import { test, expect } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const slaMs = Number(process.env.SFU_FAILOVER_SLA_MS ?? 30000);
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";
const sampleCount = Math.max(3, Number(process.env.SFU_PERF_SAMPLES ?? 8));
const sampleIntervalMs = Math.max(100, Number(process.env.SFU_PERF_SAMPLE_INTERVAL_MS ?? 500));

function percentile(values:number[],p:number){if(!values.length)return null;const sorted=[...values].sort((a,b)=>a-b);return sorted[Math.min(sorted.length-1,Math.ceil((p/100)*sorted.length)-1)];}
async function sample(endpoint:string){const started=performance.now();const response=await fetch(endpoint+"/health");const latency=performance.now()-started;if(!response.ok)throw new Error("health unavailable: "+endpoint);return latency;}

test("generate machine-readable SFU performance report",async()=>{
  test.skip(!process.env.SFU_FAILOVER_LIVE,"Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");
  const samples:number[]=[],primarySamples:number[]=[],secondarySamples:number[]=[];
  for(let i=0;i<sampleCount;i++){
    for(const [url,target] of [[primary,primarySamples],[secondary,secondarySamples]] as const){try{const value=await sample(url);samples.push(value);target.push(value);}catch{}}
    if(i+1<sampleCount)await new Promise(resolve=>setTimeout(resolve,sampleIntervalMs));
  }
  const report={generatedAt:new Date().toISOString(),target:{primary,secondary},sampleCount:samples.length,configuredSamples:sampleCount,sampleIntervalMs,samples:{all:samples,primary:primarySamples,secondary:secondarySamples},healthLatencyMs:{p50:percentile(samples,50),p95:percentile(samples,95),p99:percentile(samples,99),max:samples.length?Math.max(...samples):null},slaMs,pass:samples.length>=sampleCount*2&&(percentile(samples,95)??Infinity)<=slaMs};
  fs.mkdirSync(artifactDir,{recursive:true});
  fs.writeFileSync(path.join(artifactDir,"sfu-performance-report.json"),JSON.stringify(report,null,2));
  fs.writeFileSync(path.join(artifactDir,"sfu-performance-report.md"),`# SFU Performance Report\n\n- Samples: ${report.sampleCount}\n- p50 health latency: ${report.healthLatencyMs.p50??"n/a"} ms\n- p95 health latency: ${report.healthLatencyMs.p95??"n/a"} ms\n- p99 health latency: ${report.healthLatencyMs.p99??"n/a"} ms\n- Max health latency: ${report.healthLatencyMs.max??"n/a"} ms\n- SLA: ${slaMs} ms\n- Result: ${report.pass?"PASS":"FAIL"}\n`);
  expect(report.pass).toBeTruthy();
});