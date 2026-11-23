import type { Channel, Snapshot } from "./types";

async function send(path: string, body: unknown): Promise<void> {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const detail = await res.json().catch(() => null);
    throw new Error(detail?.detail ?? `Request failed with status ${res.status}.`);
  }
}

export async function fetchState(): Promise<Snapshot> {
  const res = await fetch("/api/state");
  if (!res.ok) throw new Error(`The engine returned status ${res.status}.`);
  return res.json();
}

export const startRun = (body: {
  actions: number;
  duration: number;
  seed: number;
  scheduled_outage: boolean;
}) => send("/api/runs", body);

export const setOutage = (channel: Channel, down: boolean) =>
  send(`/api/channels/${channel}/outage`, { down });
