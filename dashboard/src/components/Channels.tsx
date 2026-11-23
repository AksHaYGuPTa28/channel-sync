import { CHANNEL_NAMES, CHANNELS, type Channel, type ChannelState } from "../types";

function status(s: ChannelState): { text: string; tone: "ok" | "warn" | "fault" } {
  if (s.down) return { text: "Offline", tone: "fault" };
  if (s.breaker === "open") return { text: "Calls paused after repeated errors", tone: "warn" };
  if (s.breaker === "half_open") return { text: "Testing the connection", tone: "warn" };
  return { text: "Online", tone: "ok" };
}

interface Props {
  channels: Record<Channel, ChannelState>;
  canToggle: boolean;
  pending: Channel | null;
  onToggle: (ch: Channel, down: boolean) => void;
}

export function Channels({ channels, canToggle, pending, onToggle }: Props) {
  return (
    <ul className="channels">
      {CHANNELS.map((ch) => {
        const s = channels[ch];
        const st = status(s);
        return (
          <li key={ch} className={`channel ch-${ch}`}>
            <div className="channel-head">
              <h3>{CHANNEL_NAMES[ch]}</h3>
              <button
                type="button"
                className="toggle"
                disabled={!canToggle || pending === ch}
                aria-pressed={s.down}
                onClick={() => onToggle(ch, !s.down)}
              >
                {s.down ? "Bring back online" : "Take offline"}
              </button>
            </div>
            <p className={`status tone-${st.tone}`}>{st.text}</p>
            <dl className="channel-stats">
              <div>
                <dt>Bookings taken</dt>
                <dd>{s.bookings_accepted}</dd>
              </div>
              <div>
                <dt>Corrections received</dt>
                <dd>{s.corrections_applied}</dd>
              </div>
              <div>
                <dt>Webhooks sent twice</dt>
                <dd>{s.webhooks_duplicated}</dd>
              </div>
              <div>
                <dt>Webhooks lost</dt>
                <dd>{s.webhooks_dropped}</dd>
              </div>
              <div>
                <dt>Errors returned to us</dt>
                <dd>{s.api_errors_injected}</dd>
              </div>
              <div>
                <dt>Times calls were paused</dt>
                <dd>{s.breaker_opens}</dd>
              </div>
            </dl>
          </li>
        );
      })}
    </ul>
  );
}
