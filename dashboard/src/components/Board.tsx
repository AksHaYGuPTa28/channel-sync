import { memo } from "react";
import { CHANNEL_NAMES, type BoardRow, type Cell } from "../types";

const MONTH = new Intl.DateTimeFormat("en-GB", { month: "short" });
const FULL = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short" });

function nightDate(base: string, i: number): Date {
  const d = new Date(`${base}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + i);
  return d;
}

function describe(unit: string, date: Date, [owner, oversold, stale]: Cell): string {
  const parts = [`${unit}, night of ${FULL.format(date)}:`];
  parts.push(owner ? `held by ${CHANNEL_NAMES[owner]}.` : "free.");
  if (oversold) parts.push("Two guests hold this night; one is being moved or cancelled.");
  if (stale.length)
    parts.push(`Calendar not yet updated on ${stale.map((c) => CHANNEL_NAMES[c]).join(" and ")}.`);
  return parts.join(" ");
}

interface Props {
  rows: BoardRow[];
  base: string;
}

function BoardImpl({ rows, base }: Props) {
  const nightsCount = rows[0]?.cells.length ?? 0;
  const dates = Array.from({ length: nightsCount }, (_, i) => nightDate(base, i));

  return (
    <div className="board-scroll" role="region" aria-label="Availability by apartment and night" tabIndex={0}>
      <table className="board" style={{ ["--nights" as string]: nightsCount }}>
        <thead>
          <tr>
            <th scope="col" className="board-corner">
              <span className="visually-hidden">Apartment</span>
            </th>
            {dates.map((d, i) => {
              const day = d.getUTCDate();
              const monthStart = day === 1 || i === 0;
              return (
                <th
                  scope="col"
                  key={i}
                  className={monthStart ? "month-start" : undefined}
                  aria-label={FULL.format(d)}
                >
                  {monthStart && <span className="month">{MONTH.format(d)}</span>}
                  {/* columns are too narrow for every date */}
                  <span className="day" aria-hidden>
                    {monthStart || day % 5 === 0 ? day : ""}
                  </span>
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.unit}>
              <th scope="row">
                {row.unit}
                <span className="room-type">{row.room_type}</span>
              </th>
              {row.cells.map((cell, i) => {
                const [owner, oversold, stale] = cell;
                const cls = [
                  "night",
                  owner ? `own-${owner}` : "",
                  oversold ? "oversold" : "",
                  stale.length ? "stale" : "",
                ].join(" ");
                return <td key={i} className={cls} title={describe(row.unit, dates[i], cell)} />;
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export const Board = memo(BoardImpl);

export function BoardLegend() {
  return (
    <ul className="legend" aria-label="Board legend">
      <li>
        <span className="swatch night own-airbnb" />
        Airbnb guest
      </li>
      <li>
        <span className="swatch night own-booking" />
        Booking.com guest
      </li>
      <li>
        <span className="swatch night own-vrbo" />
        Vrbo guest
      </li>
      <li>
        <span className="swatch night oversold" />
        Two guests on one night, being sorted out
      </li>
      <li>
        <span className="swatch night stale" />A site's calendar is behind
      </li>
    </ul>
  );
}
