import { CHANNEL_NAMES, type ConflictRow } from "../types";

const DAY = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });

function stay(c: ConflictRow): string {
  const out = new Date(`${c.check_out}T00:00:00Z`);
  return `${DAY.format(new Date(`${c.check_in}T00:00:00Z`))} to ${DAY.format(out)}`;
}

function sentence(c: ConflictRow): string {
  const loser = CHANNEL_NAMES[c.loser];
  const winner = c.winner ? CHANNEL_NAMES[c.winner] : "another guest";
  if (c.kind === "modification_rejected") {
    return `A ${loser} guest tried to move onto nights ${winner} already sold. Their original dates were kept.`;
  }
  if (c.kind === "guest_relocated") {
    return `${winner} sold these nights first. The ${loser} guest was moved to ${c.moved_to} and keeps their booking.`;
  }
  return `${winner} sold these nights first and nothing comparable was free, so the ${loser} booking was cancelled.`;
}

export function ConflictFeed({ rows }: { rows: ConflictRow[] }) {
  if (!rows.length) {
    return <p className="empty">No clashes yet. They appear here when two sites sell the same night.</p>;
  }
  return (
    <ol className="feed" aria-live="polite">
      {rows.map((c, i) => (
        <li key={`${c.at}-${i}`}>
          <p className="feed-where">
            <strong>{c.unit}</strong>, {stay(c)}
          </p>
          <p className="feed-what">{sentence(c)}</p>
        </li>
      ))}
    </ol>
  );
}
