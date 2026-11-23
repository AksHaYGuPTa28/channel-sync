import { useEffect, useRef, useState } from "react";
import { CHANNEL_NAMES, type Channel, type Sample } from "../types";

const H = 180;

// render at real pixel width, a scaled viewBox makes the labels unreadable on phones
function useWidth<T extends HTMLElement>(fallback: number) {
  const ref = useRef<T>(null);
  const [w, setW] = useState(fallback);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(([e]) => setW(Math.max(280, Math.round(e.contentRect.width))));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return [ref, w] as const;
}
const PAD = { top: 12, right: 12, bottom: 24, left: 36 };

function niceMax(v: number): number {
  if (v <= 5) return 5;
  const p = 10 ** Math.floor(Math.log10(v));
  return Math.ceil(v / p) * p;
}

// [from, to] ranges where each channel was offline
function outageBands(samples: Sample[]): { ch: Channel; from: number; to: number }[] {
  const bands: { ch: Channel; from: number; to: number }[] = [];
  const open = new Map<Channel, number>();
  samples.forEach((s, i) => {
    for (const ch of s.down) if (!open.has(ch)) open.set(ch, s.t);
    for (const [ch, from] of open) {
      const last = i === samples.length - 1;
      if (!s.down.includes(ch) || last) {
        bands.push({ ch, from, to: s.t });
        open.delete(ch);
      }
    }
  });
  return bands;
}

export function Timeline({ samples }: { samples: Sample[] }) {
  const [ref, W] = useWidth<HTMLElement>(720);
  if (samples.length < 2) {
    return (
      <figure className="timeline" ref={ref}>
        <p className="empty">The chart fills in once a run starts.</p>
      </figure>
    );
  }
  const t0 = samples[0].t;
  const t1 = samples[samples.length - 1].t;
  const yMax = niceMax(Math.max(...samples.map((s) => Math.max(s.oversold_nights, s.queue, s.db_double))));
  const x = (t: number) => PAD.left + ((t - t0) / Math.max(t1 - t0, 1)) * (W - PAD.left - PAD.right);
  const y = (v: number) => H - PAD.bottom - (v / yMax) * (H - PAD.top - PAD.bottom);
  const line = (key: "oversold_nights" | "queue" | "db_double") =>
    samples.map((s, i) => `${i ? "L" : "M"}${x(s.t).toFixed(1)},${y(s[key]).toFixed(1)}`).join("");

  const ticks = [0, yMax / 2, yMax];
  const latest = samples[samples.length - 1];

  return (
    <figure className="timeline" ref={ref}>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        width={W}
        height={H}
        role="img"
        aria-label={
          `Over ${Math.round(t1 - t0)} seconds: nights oversold on the sites now ${latest.oversold_nights}, ` +
          `updates waiting to be sent ${latest.queue}, nights sold twice in the database ${latest.db_double}.`
        }
      >
        {outageBands(samples).map((b, i) => (
          <g key={i}>
            <rect
              className="band"
              x={x(b.from)}
              y={PAD.top}
              width={Math.max(x(b.to) - x(b.from), 2)}
              height={H - PAD.top - PAD.bottom}
            />
            <text className="band-label" x={x(b.from) + 4} y={PAD.top + 12 + (i % 2) * 14}>
              {CHANNEL_NAMES[b.ch]} offline
            </text>
          </g>
        ))}
        {ticks.map((v) => (
          <g key={v}>
            <line className="grid" x1={PAD.left} x2={W - PAD.right} y1={y(v)} y2={y(v)} />
            <text className="tick" x={PAD.left - 6} y={y(v) + 4} textAnchor="end">
              {Math.round(v)}
            </text>
          </g>
        ))}
        <text className="tick" x={PAD.left} y={H - 6}>
          {Math.round(t0)}s
        </text>
        <text className="tick" x={W - PAD.right} y={H - 6} textAnchor="end">
          {Math.round(t1)}s
        </text>
        <path className="series queue" d={line("queue")} />
        <path className="series oversold" d={line("oversold_nights")} />
        <path className="series dbdouble" d={line("db_double")} />
      </svg>
      <figcaption className="series-key">
        <span className="key oversold">Nights oversold on the sites</span>
        <span className="key queue">Updates waiting to be sent</span>
        <span className="key dbdouble">Nights sold twice in the database</span>
      </figcaption>
    </figure>
  );
}
