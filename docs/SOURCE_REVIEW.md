# Source Review Against the MVP and Chapter IV Contracts

**Review date:** 2026-11-11

**Boundary:** repository source, schemas, configuration, and automated contract
tests. The frozen 13-run campaign is a separate evidence gate; this review does
not invent its model, metrics, resource values, figures, or screenshots.

## 1. Status language

- **Implemented:** source and deterministic contract tests exist.
- **Live-verified:** a cited real MQTT or service run produced the evidence.
- **Campaign pending:** the executable path exists, but the main held-out run has
  not yet produced a result artifact for this source review.
- **Out of scope:** physical ESP32 and production deployment claims.

Ground-truth scenario events, raw data, and ML decisions remain distinct. A K2,
K3, or K4 label is not an Isolation Forest detection.

## 2. Source map and assessment

| Area | Status | Current implementation |
|---|---|---|
| MQTT simulator and scenarios | Implemented; Phase 2 path previously live-verified | Twelve independent clients by default, MQTT 3.1.1, retained status/LWT, deterministic seeds, 0-5-second initial spread, K2 5 Hz, K3 1/9-second gaps, K4 +1,024 ASCII bytes |
| Broker and ACLs | Implemented; broker integration tests exist | Anonymous access disabled, per-device ownership, separate collector/operator roles, localhost listener |
| Collector and raw schema | Implemented | Receipt-time capture, strict validation, application deduplication, schema-v3 SQLite, raw export, queue watermark, and persisted loss sidecar |
| Feature windows | Implemented | Scheduled all-device grid, 20-second/5-second `[start,end)` windows, minimum three messages, exact seven-feature schema, quality and truth kept separate |
| Isolation Forest | Implemented | Frozen initial configuration, normal-only fit, validation 1% threshold, strict `<` decision rule, checksum/compatibility-checked trusted artifact |
| Online analytics | Implemented | Per-run derived tables, every valid prediction, inference-call timing, alert subset, final reconciliation, no-model data gate |
| Experiment orchestration | Implemented; campaign pending | Canonical 13-run plan, non-overwriting manifests, isolated databases, train-to-test order, trainer hook, resource sampling, phase evidence, retries archived |
| Export and evaluation | Implemented; campaign pending | Complete scheduled-grid left join, coverage/exclusions, by-run/scenario counts, NA semantics, event detection, figures |
| Functional evidence | Implemented; live checks pending where required | T01-T16 mapping executes explicit automated nodes and consumes explicit live evidence; missing evidence becomes `not_run` |
| Dashboard | Implemented | Read-only raw/scenario views plus all predictions, score/threshold chart, model version, and alerts; no label-to-alert substitution |
| Physical hardware | Out of scope for current evidence | Protocol replacement contract exists; no ESP32 conformance run is claimed |

## 3. Key contract findings

### 3.1. Observation and label separation

Telemetry validation recursively rejects scenario/attack/label fields. Feature
construction has a fixed allow-list and consumes only canonical telemetry:
valid, non-duplicate, non-retained, session-aware `telemetry` rows ordered by
collector receipt time. Device ID, run/seed, timestamps, and ground truth remain
metadata. Actual event start/end records determine anomaly and transition labels
only after feature construction.

### 3.2. Shared feature path

`src/features/windows.py` is the one feature implementation used by analytics
and the offline pipeline. It computes message count/rate, payload population
mean/std, inter-arrival population mean/std, and peak messages per one-second
window-relative bucket. Scheduled rows are retained even for silent or
insufficient devices, preventing an inner join from hiding missing data.

### 3.3. Artifact and inference boundary

The trainer uses only the two normal train runs for fit and the distinct normal
validation run for the threshold. Metadata freezes feature order/schema,
window/stride/minimum count, Isolation Forest configuration, run IDs, library
versions, threshold direction, and model SHA-256. Loading requires an explicit
trusted path and rejects checksum or compatibility mismatches.

Analytics persists one `inference_calls` record per real batch, links every
`window_predictions` row to that call, and writes `anomaly_alerts` only for
`score < threshold`. A zero-alert result remains valid evidence; the dashboard
does not turn a scenario label into a detection.

The atomic collector sidecar prevents analytics from closing a window while an
earlier received row is still queued. Queue/storage loss marks intersecting
windows `observation_gap`; it cannot silently become a normal model input.

### 3.4. Campaign and evaluation boundary

`experiments/run_plan.csv` and `split_manifest.json` freeze 13 run IDs and unique
seeds. The runner creates one SQLite database and collection session per run,
records real UTC/monotonic timing, and prevents completed evidence from being
overwritten. Test runs require the frozen artifact.

The evaluator reconstructs every expected window for all 12 configured devices
and retains missing rows. Primary metrics require valid features, a real score,
and a normal/anomaly label. Transition and unknown rows remain exportable but do
not enter the primary confusion matrix. Scenario aggregates sum counts before
computing ratios; undefined denominators remain NA with a reason.

## 4. Evidence gates still requiring live execution

1. Run functional checks T01-T16 and retain broker/service/slow-worker evidence
   for checks that cannot pass from unit tests alone.
2. Complete both train runs and the validation run, inspect feature quality, and
   freeze the generated artifact before any test run.
3. Complete K1 and all nine K2-K4 held-out runs; keep unfavorable outcomes.
4. Verify K2-K4 behavior from observed rate, gap, and payload features rather
   than filenames or commands.
5. Export every run and generate coverage, confusion counts, Precision, Recall,
   F1, FPR, event states/latency, inference timing, and resource results.
6. Capture dashboard screenshots from a named completed run, with its database
   and timestamps retained.

Until those gates pass, the repository may claim an implemented and tested ML
pipeline, but not a trained campaign model, favorable detection quality,
real-time behavior, or generalization.

## 5. Remaining design limits

- The collector observes only broker-forwarded publications; denied
  authentication and ACL operations remain broker-log evidence.
- This is application-level MQTT simulation, not ESP32 or radio-channel
  emulation.
- SQLite and one-computer Compose are appropriate for the bounded research
  campaign, not production horizontal scale.
- Overlapping windows are correlated. Three runs per anomaly scenario are a
  preliminary lab evaluation, not strong statistical generalization.
- Joblib deserialization is safe only for the explicitly controlled artifact;
  untrusted artifacts must not be loaded.
- TLS, unique production device secrets, Internet exposure, and hardware
  provisioning require a separate security design.

## 6. Conclusion

The repository now implements the MQTT-to-SQLite path, exact scheduled feature
contract, Isolation Forest lifecycle, online decision persistence, dashboard,
campaign orchestration, and reproducible evaluation. The remaining boundary is
empirical: run the frozen plan and report only the generated `results/`
evidence.
