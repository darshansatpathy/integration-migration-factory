"""imf: inventory, generate, regress and run migrated flows."""

from __future__ import annotations

import argparse
import csv
import sys
import tempfile
import time
from pathlib import Path

import yaml

from imf import __version__, generate, harness, inventory, legacy, runtime, samples
from imf.model import load_estate


def _estate(args: argparse.Namespace):  # type: ignore[no-untyped-def]
    return load_estate(Path(args.estate))


def cmd_inventory(args: argparse.Namespace) -> int:
    entries = inventory.build(_estate(args))
    md = inventory.markdown(entries)
    if args.out:
        Path(args.out).write_text(md, encoding="utf-8")
    else:
        sys.stdout.write(md)
    if args.csv:
        rows = inventory.as_rows(entries)
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    estate, dest = _estate(args), Path(args.out)
    if args.check:
        problems = generate.drift(estate, dest)
        for p in problems:
            print(f"drift: {p}", file=sys.stderr)
        if problems:
            print("generated/ is out of date: run `imf generate` and commit the result", file=sys.stderr)
            return 1
        print(f"generated/ matches the estate and templates ({len(estate.flows)} flows)")
        return 0
    paths = generate.write(estate, dest)
    print(f"wrote {len(paths)} files for {len(estate.flows)} flows to {dest}")
    return 0


def cmd_samples(args: argparse.Namespace) -> int:
    """Write sample inputs and the legacy engine's golden outputs (or check they are reproducible)."""
    estate = _estate(args)
    before = {}
    if args.check:
        for f in estate.flows.values():
            for p in (samples.sample_path(estate, f), samples.golden_path(estate, f)):
                before[p] = p.read_bytes() if p.exists() else b""
    samples.write_samples(estate)
    for f in estate.flows.values():
        n = legacy.run(f, estate, samples.sample_path(estate, f), samples.golden_path(estate, f))
        if not args.check:
            print(f"{f.id}: {n} golden records")
    if args.check:
        changed = [str(p) for p, b in before.items() if p.read_bytes() != b]
        for p, b in before.items():
            p.write_bytes(b)
        if changed:
            print("not reproducible: " + ", ".join(changed), file=sys.stderr)
            return 1
        print("samples and golden outputs are reproducible")
    return 0


def _bus(args: argparse.Namespace) -> runtime.Bus:
    if args.bus == "kafka":
        return runtime.KafkaBus(args.bootstrap, prefix=args.prefix or f"imf-{int(time.time())}.")
    return runtime.MemoryBus()


def cmd_regress(args: argparse.Namespace) -> int:
    estate = _estate(args)
    rules = harness.load_accepted(Path(args.accepted))
    bus = _bus(args)
    flows = [estate.flows[args.flow]] if args.flow else list(estate.flows.values())
    with tempfile.TemporaryDirectory() as tmp:
        results = [harness.run_flow(f, estate, Path(args.generated), bus, Path(tmp), rules) for f in flows]
    text = harness.report(results, rules, "Kafka" if args.bus == "kafka" else "in-memory topics")
    if args.report:
        Path(args.report).write_text(text, encoding="utf-8")
    for r in results:
        status = "pass" if r.passed else "FAIL"
        print(
            f"{r.flow:9} {status}  legacy {r.legacy_records:4}  new {r.new_records:4}  differences {len(r.differences):4}  unexplained {len(r.unexplained)}"
        )
        for o in r.over_limit:
            print(f"          over limit: {o}")
        for d in r.unexplained[:5]:
            print(f"          {d.kind} {d.key} {d.field} legacy={d.legacy!r} new={d.new!r} {d.reason}")
    return 0 if all(r.passed for r in results) else 1


def cmd_compare(args: argparse.Namespace) -> int:
    """Compare a legacy output with a shadow output of the Kafka path (parallel run)."""
    estate = _estate(args)
    flow = estate.flows[args.flow]
    rules = [r for r in harness.load_accepted(Path(args.accepted)) if flow.id in r["flows"]]
    diffs, order_ok = harness.compare(flow, estate, Path(args.legacy), Path(args.new), [])
    over = harness.explain(diffs, rules)
    unexplained = [d for d in diffs if d.accepted_by is None]
    print(
        f"{flow.id}: {len(diffs)} differences, {len(diffs) - len(unexplained)} accepted, {len(unexplained)} unexplained; order {'same' if order_ok else 'differs'}"
    )
    for o in over:
        print(f"  over limit: {o}")
    for d in unexplained[:20]:
        print(f"  {d.kind} {d.key} {d.field} legacy={d.legacy!r} new={d.new!r}")
    return 0 if not unexplained and not over else 1


def cmd_run(args: argparse.Namespace) -> int:
    gen = Path(args.generated)
    ing = generate.load_spec(gen, args.flow, "ingest.yaml")
    dl = generate.load_spec(gen, args.flow, "deliver.yaml")
    schema = generate.load_spec(gen, args.flow, "schema.json")
    bus = _bus(args)
    res = runtime.ingest(ing, schema, Path(args.input), bus)
    print(
        f"ingest: read {res.read}, filtered {res.filtered}, published {res.published}, rejected {res.rejected or 0} (batch {res.batch_id})"
    )
    out = runtime.deliver(dl, bus, Path(args.output), res.batch_id)
    print(f"deliver: wrote {out.records} records to {args.output}; duplicates dropped {out.duplicates}; truncated {out.truncated or 0}")
    return 0


def cmd_topics(args: argparse.Namespace) -> int:
    """Create every generated topic, capping replication at the broker count."""
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.cimpl import NewTopic

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    meta = admin.list_topics(timeout=10)
    brokers = len(meta.brokers)
    specs = []
    for p in [*sorted(Path(args.generated).glob("*/topics.yaml")), Path(args.generated) / "platform.yaml"]:
        specs += yaml.safe_load(p.read_text())["topics"]
    new = [
        NewTopic(
            args.prefix + t["name"],
            num_partitions=t["partitions"],
            replication_factor=min(t["replication_factor"], brokers),
            config={k: v for k, v in t["config"].items() if not (k == "min.insync.replicas" and brokers < 2)},
        )
        for t in specs
        if args.prefix + t["name"] not in meta.topics
    ]
    for name, fut in admin.create_topics(new).items() if new else []:
        fut.result()
        print(f"created {name}")
    print(f"{len(specs)} topics defined, {len(new)} created, {brokers} broker(s)")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="imf", description=__doc__)
    p.add_argument("--version", action="version", version=f"imf {__version__}")
    p.add_argument("--estate", default="estate")
    p.add_argument("--generated", default="generated")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("inventory", help="catalog, complexity and migration waves")
    s.add_argument("--out")
    s.add_argument("--csv")
    s.set_defaults(fn=cmd_inventory)

    s = sub.add_parser("generate", help="render topics, schemas, ingest and deliver specs from templates")
    s.add_argument("--out", default="generated")
    s.add_argument("--check", action="store_true", help="fail if the committed files differ")
    s.set_defaults(fn=cmd_generate)

    s = sub.add_parser("samples", help="write sample inputs and legacy golden outputs")
    s.add_argument("--check", action="store_true")
    s.set_defaults(fn=cmd_samples)

    for name, fn, helptext in (("regress", cmd_regress, "compare legacy and Kafka outputs"), ("run", cmd_run, "run one flow end to end")):
        s = sub.add_parser(name, help=helptext)
        s.add_argument("--bus", choices=["memory", "kafka"], default="memory")
        s.add_argument("--bootstrap", default="127.0.0.1:9092")
        s.add_argument("--prefix", default="")
        s.set_defaults(fn=fn)
        if name == "regress":
            s.add_argument("--flow")
            s.add_argument("--accepted", default="accepted_differences.yaml")
            s.add_argument("--report")
        else:
            s.add_argument("--flow", required=True)
            s.add_argument("--input", required=True)
            s.add_argument("--output", required=True)

    s = sub.add_parser("compare", help="compare a legacy output file with a Kafka shadow output")
    s.add_argument("--flow", required=True)
    s.add_argument("--legacy", required=True)
    s.add_argument("--new", required=True)
    s.add_argument("--accepted", default="accepted_differences.yaml")
    s.set_defaults(fn=cmd_compare)

    s = sub.add_parser("topics", help="create the generated topics on a cluster")
    s.add_argument("--bootstrap", default="127.0.0.1:9092")
    s.add_argument("--prefix", default="")
    s.set_defaults(fn=cmd_topics)

    args = p.parse_args(argv)
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
