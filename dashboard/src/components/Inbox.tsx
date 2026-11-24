import { useState } from "react";
import { approveMessage, discardMessage } from "../api";
import { CHANNEL_NAMES, type Inbox as InboxState, type InboxMessage } from "../types";

const DAY = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
const day = (iso: string) => DAY.format(new Date(`${iso}T00:00:00Z`));

function drafter(by: string | null): string {
  if (!by) return "";
  if (by === "template") return "Drafted from the standard template";
  if (by.startsWith("template")) return "Model unavailable, drafted from the standard template";
  return `Drafted by ${by}`;
}

interface ItemProps {
  m: InboxMessage;
  onDone: () => void;
}

function Item({ m, onDone }: ItemProps) {
  const [body, setBody] = useState(m.body ?? "");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function act(fn: () => Promise<void>) {
    setBusy(true);
    setError(null);
    try {
      await fn();
      onDone();
    } catch (err) {
      setError((err as Error).message);
      setBusy(false);
    }
  }

  const id = `msg-${m.id}`;
  return (
    <li className="inbox-item">
      <p className="inbox-who">
        <strong>{m.guest || "Guest"}</strong> on {CHANNEL_NAMES[m.channel]}, {day(m.check_in)} to{" "}
        {day(m.check_out)}
      </p>
      <p className="inbox-move">
        {m.from_unit} → {m.to_unit}
        {m.upgrade && <span className="tag">upgrade</span>}
      </p>
      {m.guest_cancelled && (
        <p className="inbox-note tone-fault">The guest has cancelled since. Nothing to send.</p>
      )}
      <label htmlFor={id} className="visually-hidden">
        Message to {m.guest || "the guest"}
      </label>
      <textarea
        id={id}
        value={body}
        rows={7}
        disabled={busy || m.guest_cancelled}
        onChange={(e) => setBody(e.target.value)}
      />
      <p className="inbox-note">{drafter(m.drafted_by)}. Edit freely before sending.</p>
      {error && (
        <p className="inbox-note tone-fault" role="alert">
          {error}
        </p>
      )}
      <div className="inbox-actions">
        <button
          type="button"
          className="primary small"
          disabled={busy || m.guest_cancelled || !body.trim()}
          onClick={() => act(() => approveMessage(m.id, body))}
        >
          Approve and send
        </button>
        <button
          type="button"
          className="toggle"
          disabled={busy}
          onClick={() => act(() => discardMessage(m.id))}
        >
          Discard
        </button>
      </div>
    </li>
  );
}

interface Props {
  inbox: InboxState | undefined;
  onChange: () => void;
}

export function Inbox({ inbox, onChange }: Props) {
  const waiting = inbox?.counts.awaiting_approval ?? 0;
  const drafting = inbox?.counts.drafting ?? 0;
  const sent = inbox?.counts.sent ?? 0;

  return (
    <>
      <p className="inbox-summary">
        {waiting} waiting for you
        {drafting > 0 && `, ${drafting} being drafted`}
        {sent > 0 && `, ${sent} sent`}
      </p>
      {inbox?.messages.length ? (
        <ol className="inbox">
          {inbox.messages.map((m) => (
            <Item key={m.id} m={m} onDone={onChange} />
          ))}
        </ol>
      ) : (
        <p className="empty">
          When a guest is moved to another apartment, a message to them is drafted here. Nothing is sent until
          you approve it.
        </p>
      )}
    </>
  );
}
