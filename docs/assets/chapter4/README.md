# Chapter IV figure and screenshot provenance

The six analytical PNG files are generated from the measured campaign artifacts
by:

```bash
.venv/bin/python scripts/evaluate_results.py --results-dir results
```

They were copied without manual editing from `results/figures/` so the
committed Chapter IV report remains renderable while the full `results/` tree
stays local and Git-ignored. The numerical sources are
`results/evaluation_windows.csv`, `results/metrics_by_scenario.csv`, and
`results/event_detection.csv`.

Two unedited dashboard screenshots were captured on 2026-11-11 at approximately
20:54 Asia/Ho_Chi_Minh (UTC+7) from the read-only `test-k2-01` snapshot with
the **All data** filter:

- `dashboard-messages-test-k2-01.png` shows the persisted MQTT message view.
- `dashboard-ai-alerts-test-k2-01.png` shows stored model decisions, the frozen
  threshold, and the retained alert total.

They are historical-run views, not evidence of a separate live `demo-*` run.
A dedicated alert-to-source-window drill-down screenshot is not retained.
