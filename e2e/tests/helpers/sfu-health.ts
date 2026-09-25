export type SfuHealth = { cluster: boolean; nodeId: string };

const defaultClusterWaitMs = 20000;

export async function waitForClusterReady(
  endpoint: string,
  timeoutMs = Number(process.env.SFU_CLUSTER_READY_WAIT_MS ?? defaultClusterWaitMs)
): Promise<SfuHealth> {
  const deadline = Date.now() + timeoutMs;
  let lastBody: SfuHealth | null = null;
  let lastError: unknown = null;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(new URL("/health", endpoint).toString());
      if (response.ok) {
        lastBody = await response.json() as SfuHealth;
        if (lastBody.cluster === true) return lastBody;
      }
    } catch (error) {
      lastError = error;
    }
    await new Promise(resolve => setTimeout(resolve, 250));
  }

  const detail = lastBody ? ` body=${JSON.stringify(lastBody)}` : "";
  const error = lastError ? ` error=${String(lastError)}` : "";
  throw new Error(`cluster readiness timeout: ${endpoint}${detail}${error}`);
}
