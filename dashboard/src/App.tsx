import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import { fetchState, setOutage, startRun } from "./api";
import { Board, BoardLegend } from "./components/Board";
import { Channels } from "./components/Channels";
import { ConflictFeed } from "./components/ConflictFeed";
import { Timeline } from "./components/Timeline";
import type { Channel, Phase, Snapshot } from "./types";

const RUNNING: Phase[] = ["starting", "storm", "converging"];

const PHASE_TEXT: Record<Phase, string> = {
  idle: "Ready. A run sends thousands of guests at three booking sites at once, while the sites misbehave on purpose.",
  starting: "Setting up 12 apartments on three booking sites.",
  storm:
    "Guests are booking, cancelling and changing dates. The sites are dropping, repeating and delaying messages.",
  converging: "Guests have stopped. Waiting for every site to catch up with the database.",
  verified: "Run finished. Every site was checked against the database.",
  failed: "The run stopped with an error.",
};

const CHECK_TEXT: Record<string, string> = {
  no_double_bookings_in_db: "No night sold twice in the database",
  no_double_bookings_on_channels: "No night held by two guests on the sites",
  channels_agree_with_db: "Every site's bookings match the database",
  calendars_match_db: "Every site's calendar matches the database",
};

const sum = (o: Record<string, number> | undefined) => Object.values(o ?? {}).reduce((a, b) => a + b, 0);

export default function App() {
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [offline, setOffline] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState<Channel | null>(null);
  const [form, setForm] = useState({ actions: 3000, duration: 30, outage: true });
  const timer = useRef<number>();

  const poll = useCallback(async () => {
    let next = 2000;
    try {
      const s = await fetchState();
      setSnap(s);
      setOffline(false);
      if (RUNNING.includes(s.phase)) next = 500;
    } catch {
      setOffline(true);
    }
    timer.current = window.setTimeout(poll, next);
  }, []);

  useEffect(() => {
    poll();
    return () => window.clearTimeout(timer.current);
  }, [poll]);

  const phase: Phase = snap?.phase ?? "idle";
  const running = RUNNING.includes(phase);
  const latest = snap?.timeline?.at(-1);

  async function onStart(e: FormEvent) {
    e.preventDefault();
    setError(null);
    try {
      await startRun({
        actions: form.actions,
        duration: form.duration,
        seed: Date.now() % 100000,
        scheduled_outage: form.outage,
      });
      window.clearTimeout(timer.current);
      poll();
    } catch (err) {
      setError((err as Error).message);
    }
  }

  async function onToggle(ch: Channel, down: boolean) {
    setPending(ch);
    setError(null);
    try {
      await setOutage(ch, down);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setPending(null);
    }
  }

  return (
    <div className="page">
      <header className="masthead">
        <div className="title-block">
          <h1>Channel sync</h1>
          <p className="lede">
            Three booking sites sell the same 12 apartments. When two sites sell the same night, the database
            keeps the first booking and the other site is told to cancel.
          </p>
        </div>
        <form className="run-form" onSubmit={onStart}>
          <label>
            <span>Guest actions</span>
            <input
              type="number"
              min={100}
              max={20000}
              step={100}
              value={form.actions}
              disabled={running}
              onChange={(e) => setForm({ ...form, actions: Number(e.target.value) })}
            />
          </label>
          <label>
            <span>Over</span>
            <select
              value={form.duration}
              disabled={running}
              onChange={(e) => setForm({ ...form, duration: Number(e.target.value) })}
            >
              <option value={15}>15 seconds</option>
              <option value={30}>30 seconds</option>
              <option value={60}>60 seconds</option>
            </select>
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={form.outage}
              disabled={running}
              onChange={(e) => setForm({ ...form, outage: e.target.checked })}
            />
            <span>Take Booking.com offline partway through</span>
          </label>
          <button type="submit" className="primary" disabled={running || offline}>
            {running ? "Run in progress" : phase === "idle" ? "Start run" : "Start another run"}
          </button>
        </form>
      </header>

      {offline && (
        <p className="notice fault" role="alert">
          Can't reach the engine. Start it with <code>python -m sim.server</code>, then this page reconnects
          on its own.
        </p>
      )}
      {error && (
        <p className="notice fault" role="alert">
          {error}
        </p>
      )}

      <section className={`phase phase-${phase}`} aria-live="polite">
        <p>
          {PHASE_TEXT[phase]}
          {snap?.error ? ` ${snap.error}` : ""}
        </p>
        {snap?.elapsed !== undefined && running && <p className="elapsed">{Math.round(snap.elapsed)}s</p>}
      </section>

      <section className="facts" aria-label="Live numbers">
        <div className="fact hero-fact">
          <span className="fact-value">{snap?.engine?.double_bookings ?? 0}</span>
          <span className="fact-label">Nights sold twice in the database</span>
        </div>
        <div className="fact">
          <span className="fact-value tone-oversold">{latest?.oversold_nights ?? 0}</span>
          <span className="fact-label">Nights oversold on the sites right now, waiting for a correction</span>
        </div>
        <div className="fact">
          <span className="fact-value">{sum(snap?.engine?.conflicts)}</span>
          <span className="fact-label">Clashes caught and resolved</span>
        </div>
        <div className="fact">
          <span className="fact-value">{latest?.queue ?? 0}</span>
          <span className="fact-label">Updates waiting to be sent to the sites</span>
        </div>
      </section>

      {snap?.checks && (
        <section className="results" aria-label="Checks after the run">
          <h2>After the run</h2>
          <ul>
            {Object.entries(snap.checks).map(([k, c]) => (
              <li key={k} className={c.ok ? "pass" : "fail"}>
                <span className="mark" aria-hidden>
                  {c.ok ? "✓" : "✕"}
                </span>
                {CHECK_TEXT[k] ?? k}
                <span className="visually-hidden">{c.ok ? " passed" : " failed"}</span>
              </li>
            ))}
          </ul>
          <p className="results-detail">
            {sum(snap.engine?.events)} webhooks handled, {snap.engine?.events?.stale ?? 0} late ones ignored,{" "}
            {snap.healed?.missed_events_ingested ?? 0} picked up by the reconciler,{" "}
            {snap.engine?.push_retries ?? 0} pushes retried. 95% of updates reached the sites within{" "}
            {Math.round(snap.engine?.push_latency_ms.p95 ?? 0)} ms.
          </p>
        </section>
      )}

      <main className="layout">
        <div className="primary-col">
          <section aria-labelledby="board-title">
            <div className="section-head">
              <h2 id="board-title">Every apartment, every night</h2>
              <BoardLegend />
            </div>
            {snap?.board?.length && snap.base_date ? (
              <Board rows={snap.board} base={snap.base_date} />
            ) : (
              <p className="empty">Start a run to fill the calendar.</p>
            )}
          </section>
          <section aria-labelledby="timeline-title">
            <h2 id="timeline-title">Over time</h2>
            <Timeline samples={snap?.timeline ?? []} />
          </section>
        </div>

        <aside className="side-col">
          <section aria-labelledby="channels-title">
            <h2 id="channels-title">Booking sites</h2>
            {snap?.channels ? (
              <Channels
                channels={snap.channels}
                canToggle={phase === "storm"}
                pending={pending}
                onToggle={onToggle}
              />
            ) : (
              <p className="empty">
                Sites appear once a run starts. You can take any of them offline mid-run.
              </p>
            )}
          </section>
          <section aria-labelledby="feed-title">
            <h2 id="feed-title">Latest clashes</h2>
            <ConflictFeed rows={snap?.conflicts ?? []} />
          </section>
        </aside>
      </main>

      <footer className="foot">
        Booking sites are simulated. The database, sync engine and reconciler are the real code from this
        repository.
      </footer>
    </div>
  );
}
