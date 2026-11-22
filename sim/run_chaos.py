"""Run the chaos scenario and print a report. Exits 1 if any check fails.

python -m sim.run_chaos --actions 3000 --seed 7
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from sim.scenario import CHANNELS, ChaosRun, Params

REPORT_PATH = Path(__file__).resolve().parent.parent / "reports" / "last_run.json"


async def main(params: Params) -> int:
    run = ChaosRun(params)
    print(
        f"{params.actions} guest actions over ~{params.duration}s, "
        f"{params.units} units x {len(CHANNELS)} channels, seed {params.seed}"
    )
    try:
        report = await run.run()
    finally:
        await run.close()

    REPORT_PATH.parent.mkdir(exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2, default=str))
    print_report(report)
    return 0 if all(c["ok"] for c in report["checks"].values()) else 1


def print_report(r: dict) -> None:
    e = r["engine"]
    print()
    print(f"workload      {r['workload_seconds']}s, settled in {r['convergence_seconds']}s")
    print(f"guests        {r['guest_actions']}")
    for name, s in r["channels"].items():
        print(
            f"{name:<13} accepted={s['bookings_accepted']} dup={s['webhooks_duplicated']} "
            f"dropped={s['webhooks_dropped']} errors={s['api_errors_injected']} "
            f"corrections={s['corrections_applied']}"
        )
    print(f"events        {e['events']}")
    print(f"conflicts     {e['conflicts']}")
    print(f"reservations  {e['reservations']}")
    print(f"outbox        {e['outbox']} retries={e['push_retries']}")
    print(f"push ms       {e['push_latency_ms']}")
    print(f"breaker trips {r['circuit_breaker_opens']}")
    print(f"reconciler    {r['healed_by_reconciler']}")
    print()
    for name, c in r["checks"].items():
        detail = {k: v for k, v in c.items() if k != "ok"}
        print(f"{'PASS' if c['ok'] else 'FAIL'}  {name}  {detail}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--actions", type=int, default=3000)
    ap.add_argument("--units", type=int, default=12)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--duration", type=float, default=15.0)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    if sys.platform == "win32":
        # asyncpg is happier on the selector loop
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    sys.exit(asyncio.run(main(Params(**vars(args)))))
