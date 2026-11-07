# Contributing

```bash
docker compose up -d
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
export IMF_BOOTSTRAP=localhost:9092
make check
```

After changing a flow, a layout, a template or a sample generator, run `make regenerate` and commit
`generated/`, `estate/samples/`, `estate/golden/` and `docs/inventory.*` with the change.

A change that alters any flow's output must either keep `imf regress` passing unchanged, or add an
entry to `accepted_differences.yaml` with a rationale, the approving owner, a ticket and a limit.
