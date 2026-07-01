#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cannbot-usage-aggregate.py
==========================

Aggregate CANNBOT proxy token usage across multiple servers.

Two modes:
  1. **Remote mode (default)**: SSH into each host, fetch its ``/_usage``
     endpoint (or read its ``usage.jsonl`` directly), and merge.
  2. **Local mode** (``--local FILE [FILE ...]``): merge one or more
     ``usage.jsonl`` files that you've already collected.

Usage
-----
Remote mode — provide hosts as ``user@host:port`` (port = proxy port)::

    ./cannbot-usage-aggregate.py \\
        alice@server1:8765 \\
        bob@server2:8765 \\
        server3:8765

    # Or read hosts from a file (one per line, ``#`` for comments)
    ./cannbot-usage-aggregate.py --hosts-file hosts.txt

    # Override SSH options
    ./cannbot-usage-aggregate.py --ssh "ssh -i ~/.ssh/id_ed25519" server1:8765

Local mode::

    # Pull usage.jsonl from each box yourself, then merge:
    scp server1:~/.cannbot/proxy/usage.jsonl /tmp/usage-s1.jsonl
    scp server2:~/.cannbot/proxy/usage.jsonl /tmp/usage-s2.jsonl
    ./cannbot-usage-aggregate.py --local /tmp/usage-s1.jsonl /tmp/usage-s2.jsonl

Output
------
By default prints a human-readable table. Use ``--json`` for machine-readable
JSON output (suitable for piping into ``jq`` or feeding a dashboard).
"""

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple


def _parse_host(spec: str) -> Tuple[str, int]:
    """Parse ``user@host:port`` into (ssh_target, port)."""
    if ":" in spec:
        target, port_str = spec.rsplit(":", 1)
        port = int(port_str)
    else:
        target = spec
        port = 8765
    return target, port


def _fetch_remote_via_ssh(ssh_cmd: str, target: str, port: int, use_api: bool) -> List[dict]:
    """Fetch usage records from a remote host.

    If *use_api* is True, curl the proxy's ``/_usage`` endpoint over SSH.
    Otherwise, read ``~/.cannbot/proxy/usage.jsonl`` directly via SSH cat.
    """
    if use_api:
        remote = f"curl -sS http://127.0.0.1:{port}/_usage?recent=99999"
    else:
        remote = "cat ~/.cannbot/proxy/usage.jsonl 2>/dev/null"

    full_cmd = f"{ssh_cmd} {target} '{remote}'"
    try:
        result = subprocess.run(
            full_cmd, shell=True, capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            print(f"[WARN] SSH to {target} failed: {result.stderr.strip()}", file=sys.stderr)
            return []
        if use_api:
            data = json.loads(result.stdout)
            return data.get("recent", [])
        else:
            records = []
            for line in result.stdout.strip().splitlines():
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
            return records
    except subprocess.TimeoutExpired:
        print(f"[WARN] SSH to {target} timed out (30s)", file=sys.stderr)
        return []
    except (json.JSONDecodeError, Exception) as e:
        print(f"[WARN] Error parsing response from {target}: {e}", file=sys.stderr)
        return []


def _read_local_file(path: str) -> List[dict]:
    """Read a local usage.jsonl file."""
    records = []
    if not os.path.isfile(path):
        print(f"[WARN] File not found: {path}", file=sys.stderr)
        return records
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def _aggregate(records: List[dict]) -> dict:
    """Merge usage records into a summary."""
    by_host: Dict[str, dict] = defaultdict(lambda: {
        "requests": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0
    })
    by_model: Dict[str, dict] = defaultdict(lambda: {
        "requests": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0
    })
    by_day: Dict[str, dict] = defaultdict(lambda: {
        "requests": 0, "total_tokens": 0
    })
    by_host_model: Dict[str, Dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: {
        "requests": 0, "total_tokens": 0
    }))

    total = {"requests": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    for r in records:
        host = r.get("host", "unknown")
        model = r.get("model", "unknown")
        pt = r.get("prompt_tokens", 0)
        ct = r.get("completion_tokens", 0)
        tt = r.get("total_tokens", 0)
        day = r.get("ts", "")[:10]

        by_host[host]["requests"] += 1
        by_host[host]["prompt_tokens"] += pt
        by_host[host]["completion_tokens"] += ct
        by_host[host]["total_tokens"] += tt

        by_model[model]["requests"] += 1
        by_model[model]["prompt_tokens"] += pt
        by_model[model]["completion_tokens"] += ct
        by_model[model]["total_tokens"] += tt

        by_day[day]["requests"] += 1
        by_day[day]["total_tokens"] += tt

        by_host_model[host][model]["requests"] += 1
        by_host_model[host][model]["total_tokens"] += tt

        total["requests"] += 1
        total["prompt_tokens"] += pt
        total["completion_tokens"] += ct
        total["total_tokens"] += tt

    return {
        "total": total,
        "by_host": dict(by_host),
        "by_model": dict(by_model),
        "by_day": dict(by_day),
        "by_host_model": {h: dict(m) for h, m in by_host_model.items()},
        "record_count": len(records),
    }


def _print_table(summary: dict) -> None:
    """Print a human-readable summary table."""
    total = summary["total"]
    print("=" * 70)
    print(f"  CANNBOT Usage Summary  ({summary['record_count']} records)")
    print("=" * 70)
    print()
    print(f"  Total requests        : {total['requests']:,}")
    print(f"  Total prompt tokens   : {total['prompt_tokens']:,}")
    print(f"  Total completion      : {total['completion_tokens']:,}")
    print(f"  Total tokens          : {total['total_tokens']:,}")
    print()

    print("-" * 70)
    print("  By Host")
    print("-" * 70)
    print(f"  {'Host':<20} {'Requests':>10} {'Prompt':>12} {'Completion':>12} {'Total':>12}")
    for host, v in sorted(summary["by_host"].items()):
        print(f"  {host:<20} {v['requests']:>10,} {v['prompt_tokens']:>12,} "
              f"{v['completion_tokens']:>12,} {v['total_tokens']:>12,}")
    print()

    print("-" * 70)
    print("  By Model")
    print("-" * 70)
    print(f"  {'Model':<25} {'Requests':>10} {'Prompt':>12} {'Completion':>12} {'Total':>12}")
    for model, v in sorted(summary["by_model"].items()):
        print(f"  {model:<25} {v['requests']:>10,} {v['prompt_tokens']:>12,} "
              f"{v['completion_tokens']:>12,} {v['total_tokens']:>12,}")
    print()

    print("-" * 70)
    print("  By Day")
    print("-" * 70)
    print(f"  {'Date':<12} {'Requests':>10} {'Total tokens':>15}")
    for day, v in sorted(summary["by_day"].items()):
        print(f"  {day:<12} {v['requests']:>10,} {v['total_tokens']:>15,}")
    print()

    print("-" * 70)
    print("  By Host x Model")
    print("-" * 70)
    print(f"  {'Host':<18} {'Model':<22} {'Requests':>10} {'Total':>12}")
    for host, models in sorted(summary["by_host_model"].items()):
        for model, v in sorted(models.items()):
            print(f"  {host:<18} {model:<22} {v['requests']:>10,} {v['total_tokens']:>12,}")
    print()


def main() -> None:
    p = argparse.ArgumentParser(
        prog="cannbot-usage-aggregate",
        description="Aggregate CANNBOT proxy usage across multiple servers.",
    )
    p.add_argument("hosts", nargs="*", help="Remote hosts as user@host:port (port=proxy port).")
    p.add_argument("--hosts-file", help="File with one host per line (# for comments).")
    p.add_argument("--ssh", default="ssh", help="SSH command (default: ssh).")
    p.add_argument("--local", nargs="+", help="Local usage.jsonl files to merge instead of SSH.")
    p.add_argument("--cat", action="store_true",
                   help="Read usage.jsonl via SSH cat instead of the /_usage API "
                        "(use if the proxy isn't running on the remote).")
    p.add_argument("--json", action="store_true", help="Output JSON instead of a table.")
    args = p.parse_args()

    all_records: List[dict] = []

    if args.local:
        for path in args.local:
            all_records.extend(_read_local_file(path))
    else:
        hosts = list(args.hosts)
        if args.hosts_file:
            with open(args.hosts_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        hosts.append(line)
        if not hosts:
            p.error("No hosts specified. Use positional args, --hosts-file, or --local.")

        use_api = not args.cat
        for spec in hosts:
            target, port = _parse_host(spec)
            print(f"[INFO] Fetching from {target} (port {port}, "
                  f"mode={'api' if use_api else 'cat'})...", file=sys.stderr)
            records = _fetch_remote_via_ssh(args.ssh, target, port, use_api)
            print(f"       → {len(records)} records", file=sys.stderr)
            all_records.extend(records)

    summary = _aggregate(all_records)

    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        _print_table(summary)


if __name__ == "__main__":
    main()
