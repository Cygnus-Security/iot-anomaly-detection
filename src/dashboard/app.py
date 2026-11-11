"""Live Streamlit security dashboard backed by real collector observations."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from src.dashboard.data import DashboardDataError, DashboardRepository

DATABASE_PATH = os.getenv("SQLITE_PATH", "data/runtime/iot_anomaly.db")

st.set_page_config(
    page_title="IoT Anomaly Detection",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.7rem; padding-bottom: 2rem; max-width: 1500px;}
    [data-testid="stMetric"] {
        background: linear-gradient(145deg, rgba(20,48,74,.92), rgba(12,31,51,.92));
        border: 1px solid rgba(75, 190, 190, .22);
        padding: 1rem 1.1rem;
        border-radius: 14px;
        min-height: 118px;
    }
    [data-testid="stMetricLabel"] {color: #A8BACD;}
    [data-testid="stMetricValue"] {color: #F3F8FC;}
    div[data-testid="stDataFrame"] {border: 1px solid rgba(93,130,160,.20); border-radius: 12px;}
    .live-dot {
        display:inline-block; width:9px; height:9px; border-radius:50%;
        background:#2DD4BF; box-shadow:0 0 0 5px rgba(45,212,191,.12);
        margin-right:8px;
    }
    .eyebrow {color:#6EE7D8; letter-spacing:.12em; font-size:.78rem; font-weight:700;}
    .muted {color:#8FA5B9;}
    </style>
    """,
    unsafe_allow_html=True,
)

repository = DashboardRepository(DATABASE_PATH)
try:
    repository.check_ready()
except DashboardDataError as exc:
    st.error(f"Unable to initialize the dashboard: {exc}")
    st.info("Start the collector first with `docker compose up -d collector`.")
    st.stop()

st.markdown('<div class="eyebrow">AIoT SECURITY LAB</div>', unsafe_allow_html=True)
st.title("IoT Anomaly Detection System")
st.caption(
    "Live view of MQTT data accepted by the broker and stored in SQLite by the collector."
)

WINDOWS = {
    "Last 5 minutes": 300,
    "Last 15 minutes": 900,
    "Last hour": 3600,
    "All data": 0,
}

with st.sidebar:
    st.header("Display controls")
    auto_refresh = st.toggle("Auto-refresh", value=True)
    refresh_seconds = st.select_slider(
        "Refresh interval (seconds)", options=[2, 5, 10, 30], value=5
    )
    window_label = st.selectbox("Time range", list(WINDOWS), index=1)
    current_devices = repository.devices(window_seconds=WINDOWS[window_label])
    device_options = ["All devices"] + current_devices["device_id"].tolist()
    selected_label = st.selectbox("Device", device_options)
    selected_device = None if selected_label == "All devices" else selected_label
    st.button("Refresh now", width="stretch")
    st.divider()
    st.caption("Source: live SQLite · read-only")
    st.caption("Data timezone: UTC")

run_every = f"{refresh_seconds}s" if auto_refresh else None


def state_label(value: str) -> str:
    return {
        "online": "Online",
        "stale": "Stale",
        "offline": "Offline",
        "unknown": "Unknown",
    }.get(value, value)


def numeric_or_zero(value: object) -> float:
    """Normalize nullable aggregate values before displaying a metric."""

    return float(value) if pd.notna(value) else 0.0


@st.fragment(run_every=run_every)
def live_dashboard() -> None:
    try:
        now = datetime.now(timezone.utc).timestamp()
        overview = repository.overview(now)
        devices = repository.devices(
            window_seconds=WINDOWS[window_label], now_unix=now
        )
        traffic = repository.traffic_series(
            window_seconds=WINDOWS[window_label],
            device_id=selected_device,
            now_unix=now,
        )
        labels = repository.label_counts(
            window_seconds=WINDOWS[window_label],
            device_id=selected_device,
            now_unix=now,
        )
        prediction_table_ready = repository.has_prediction_table()
        alert_table_ready = repository.has_detection_table()
        prediction_count = repository.prediction_count() if prediction_table_ready else 0
        alert_count = repository.alert_count() if alert_table_ready else 0
    except DashboardDataError as exc:
        st.error(f"Unable to refresh dashboard data: {exc}")
        return

    active_count = int((devices["observed_state"] == "online").sum())
    latest = overview.latest_received_at or "No data"
    st.markdown(
        f'<span class="live-dot"></span><b>Live</b> '
        f'<span class="muted">· Latest message: {latest}</span>',
        unsafe_allow_html=True,
    )

    metrics = st.columns(6)
    metrics[0].metric("Active devices", f"{active_count}/{overview.device_count}")
    metrics[1].metric("Total telemetry", f"{overview.telemetry_count:,}")
    metrics[2].metric(
        "60-second rate",
        f"{overview.messages_per_second_last_minute:.2f} msg/s",
        help=f"{overview.messages_last_minute} telemetry messages in the last 60 seconds",
    )
    metrics[3].metric("Invalid payloads", f"{overview.invalid_payload_count:,}")
    metrics[4].metric(
        "Abnormal scenarios",
        f"{overview.abnormal_scenario_runs:,}",
        help="K2-K4 ground truth, not a Machine Learning result",
    )
    metrics[5].metric(
        "AI alerts",
        "Unavailable" if not prediction_table_ready else f"{alert_count:,}",
        help=(
            "Alerts are model decisions below the frozen threshold; all normal and "
            "anomalous decisions remain available in the AI & Alerts tab."
        ),
    )

    overview_tab, devices_tab, scenarios_tab, ai_tab = st.tabs(
        ["Overview", "Devices", "Experiment Scenarios", "AI & Alerts"]
    )

    with overview_tab:
        left, right = st.columns([1.75, 1])
        with left:
            st.subheader("MQTT telemetry traffic")
            if traffic.empty:
                st.info("No telemetry is available for the selected time range.")
            else:
                st.line_chart(
                    traffic,
                    x="bucket_at",
                    y="messages",
                    x_label="Time (UTC)",
                    y_label="Messages per bucket",
                    height=330,
                )
        with right:
            st.subheader("Experiment label distribution")
            st.caption(
                "Labels come from the scenario controller and are not ML inputs."
            )
            if labels.empty:
                st.info("No labels are available for the selected time range.")
            else:
                st.bar_chart(
                    labels,
                    x="ground_truth_label",
                    y="messages",
                    x_label="Ground truth",
                    y_label="Telemetry",
                    height=300,
                )

        st.subheader("Observed device status")
        if devices.empty:
            st.info("The collector has not observed any devices yet.")
        else:
            display_devices = devices.copy()
            display_devices["observed_state"] = display_devices["observed_state"].map(
                state_label
            )
            display_devices = display_devices.rename(
                columns={
                    "device_id": "Device",
                    "observed_state": "State",
                    "age_seconds": "Observation age (s)",
                    "telemetry_in_window": "Telemetry",
                    "avg_payload_bytes": "Avg payload (bytes)",
                    "avg_inter_arrival_seconds": "Avg inter-arrival (s)",
                    "last_sequence": "Last sequence",
                    "last_seen_at": "Last observed at",
                }
            )
            st.dataframe(
                display_devices[
                    [
                        "Device",
                        "State",
                        "Observation age (s)",
                        "Telemetry",
                        "Avg payload (bytes)",
                        "Avg inter-arrival (s)",
                        "Last sequence",
                        "Last observed at",
                    ]
                ],
                hide_index=True,
                width="stretch",
                height=390,
            )

    with devices_tab:
        if selected_device is None:
            st.info("Select a device in the sidebar to view detailed analysis.")
            latest_messages = repository.latest_observations(limit=100)
            st.subheader("Latest 100 messages")
            st.dataframe(
                latest_messages,
                hide_index=True,
                width="stretch",
                height=520,
            )
        else:
            history = repository.device_telemetry(selected_device)
            device_row = devices.loc[devices["device_id"] == selected_device]
            st.subheader(f"Device details: {selected_device}")
            if not device_row.empty:
                row = device_row.iloc[0]
                cols = st.columns(4)
                cols[0].metric("State", state_label(str(row["observed_state"])))
                cols[1].metric(
                    "Last sequence", int(numeric_or_zero(row["last_sequence"]))
                )
                cols[2].metric(
                    "Average payload",
                    f"{numeric_or_zero(row['avg_payload_bytes']):.1f} bytes",
                )
                cols[3].metric(
                    "Average inter-arrival",
                    f"{numeric_or_zero(row['avg_inter_arrival_seconds']):.3f} s",
                )
            if history.empty:
                st.info("This device has no valid telemetry yet.")
            else:
                chart_left, chart_right = st.columns(2)
                with chart_left:
                    st.markdown("**Payload size**")
                    st.line_chart(
                        history,
                        x="received_at",
                        y="payload_size_bytes",
                        color="ground_truth_label",
                        height=300,
                    )
                with chart_right:
                    st.markdown("**Inter-arrival time**")
                    interval_data = history.dropna(subset=["inter_arrival_seconds"])
                    st.line_chart(
                        interval_data,
                        x="received_at",
                        y="inter_arrival_seconds",
                        color="ground_truth_label",
                        height=300,
                    )
                sensor_left, sensor_right = st.columns(2)
                with sensor_left:
                    st.markdown("**Simulated temperature**")
                    st.line_chart(
                        history, x="received_at", y="temperature_c", height=250
                    )
                with sensor_right:
                    st.markdown("**Simulated relative humidity**")
                    st.line_chart(
                        history, x="received_at", y="humidity_percent", height=250
                    )
                st.dataframe(
                    history.tail(100).iloc[::-1],
                    hide_index=True,
                    width="stretch",
                    height=360,
                )

    with scenarios_tab:
        st.subheader("K1-K4 history")
        st.caption(
            "Each run records its request ID, run ID, and observed start/end times."
        )
        runs = repository.scenario_runs()
        if runs.empty:
            st.info("No experiment scenario has been run yet.")
        else:
            scenario_summary = (
                runs.groupby("scenario", as_index=False)
                .size()
                .rename(columns={"scenario": "Scenario", "size": "Runs"})
            )
            st.bar_chart(
                scenario_summary,
                x="Scenario",
                y="Runs",
                height=260,
            )
            display_runs = runs.rename(
                columns={
                    "device_id": "Device",
                    "scenario": "Scenario",
                    "started_at": "Started at",
                    "ended_at": "Ended at",
                    "configured_duration_seconds": "Configured duration (s)",
                    "actual_duration_seconds": "Observed duration (s)",
                    "seed": "Seed",
                    "run_state": "State",
                    "scenario_run_id": "Run ID",
                }
            )
            st.dataframe(
                display_runs[
                    [
                        "Device",
                        "Scenario",
                        "State",
                        "Started at",
                        "Ended at",
                        "Configured duration (s)",
                        "Observed duration (s)",
                        "Seed",
                        "Run ID",
                    ]
                ],
                hide_index=True,
                width="stretch",
                height=420,
            )

    with ai_tab:
        st.subheader("Isolation Forest & alerts")
        if prediction_table_ready:
            try:
                predictions = repository.predictions(device_id=selected_device)
                alerts = repository.detections()
            except DashboardDataError as exc:
                st.error(
                    f"The Phase 3 tables do not match their contract: {exc}"
                )
            else:
                affected_devices = int(alerts["device_id"].nunique())
                col1, col2, col3 = st.columns(3)
                col1.metric("Window predictions", prediction_count)
                col2.metric("Anomaly alerts", len(alerts))
                col3.metric("Affected devices", affected_devices)
                st.caption(
                    "An alert is created only when decision_function score is below "
                    "the validation-calibrated threshold. Ground truth is shown only "
                    "for evaluation and is not a model input."
                )
                if predictions.empty:
                    st.info(
                        "The analytics schema is ready, but this database contains no "
                        "model predictions. Training and validation runs intentionally "
                        "collect features without inference."
                    )
                else:
                    timeline = predictions.sort_values("window_end_utc")
                    st.markdown("**Anomaly score and frozen threshold**")
                    st.line_chart(
                        timeline,
                        x="window_end_utc",
                        y=["anomaly_score", "decision_threshold"],
                        x_label="Window end (UTC)",
                        y_label="decision_function",
                        height=320,
                    )
                    st.markdown("**All model decisions**")
                    st.dataframe(
                        predictions,
                        hide_index=True,
                        width="stretch",
                        height=400,
                    )
                st.markdown("**Alerts below the threshold**")
                if alerts.empty:
                    st.info("No anomaly alert was produced for this database.")
                else:
                    st.dataframe(
                        alerts,
                        hide_index=True,
                        width="stretch",
                        height=360,
                    )
        else:
            st.warning(
                "The `window_predictions` table does not exist yet. The dashboard "
                "does not fabricate scores or alerts."
            )
            st.markdown(
                "**Abnormal ground truth available for future training and evaluation**"
            )
            abnormal_runs = repository.scenario_runs()
            abnormal_runs = abnormal_runs.loc[abnormal_runs["scenario"] != "normal"]
            if abnormal_runs.empty:
                st.info("The dataset does not contain any K2-K4 runs yet.")
            else:
                st.dataframe(
                    abnormal_runs[
                        [
                            "device_id",
                            "scenario",
                            "started_at",
                            "ended_at",
                            "actual_duration_seconds",
                            "scenario_run_id",
                        ]
                    ],
                    hide_index=True,
                    width="stretch",
                )


live_dashboard()
