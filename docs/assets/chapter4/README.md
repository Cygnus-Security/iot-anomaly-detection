# Chapter IV figure provenance

These six PNG files are generated from the measured campaign artifacts by:

```bash
.venv/bin/python scripts/evaluate_results.py --results-dir results
```

They were copied without manual editing from `results/figures/` so the
committed Chapter IV report remains renderable while the full `results/` tree
stays local and Git-ignored. The numerical sources are
`results/evaluation_windows.csv`, `results/metrics_by_scenario.csv`, and
`results/event_detection.csv`.

Dashboard screenshots are not included because no controllable browser was
connected during the documentation session. They must be captured from the
read-only `test-k2-01` snapshot and identified as historical-run views, not a
separate live `demo-*` run.
