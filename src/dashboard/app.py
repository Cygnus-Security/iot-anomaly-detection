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
    st.error(f"Không thể khởi tạo dashboard: {exc}")
    st.info("Hãy khởi động collector trước bằng `docker compose up -d collector`.")
    st.stop()

st.markdown('<div class="eyebrow">AIoT SECURITY LAB</div>', unsafe_allow_html=True)
st.title("IoT Anomaly Detection System")
st.caption(
    "Quan sát trực tiếp dữ liệu MQTT đã được broker chấp nhận và collector lưu vào SQLite."
)

WINDOWS = {
    "5 phút gần nhất": 300,
    "15 phút gần nhất": 900,
    "1 giờ gần nhất": 3600,
    "Toàn bộ dữ liệu": 0,
}

with st.sidebar:
    st.header("Điều khiển hiển thị")
    auto_refresh = st.toggle("Tự động cập nhật", value=True)
    refresh_seconds = st.select_slider(
        "Chu kỳ cập nhật (giây)", options=[2, 5, 10, 30], value=5
    )
    window_label = st.selectbox("Khoảng thời gian", list(WINDOWS), index=1)
    current_devices = repository.devices(window_seconds=WINDOWS[window_label])
    device_options = ["Tất cả thiết bị"] + current_devices["device_id"].tolist()
    selected_label = st.selectbox("Thiết bị", device_options)
    selected_device = None if selected_label == "Tất cả thiết bị" else selected_label
    st.button("Làm mới ngay", width="stretch")
    st.divider()
    st.caption("Nguồn: SQLite live · chỉ đọc")
    st.caption("Múi giờ dữ liệu: UTC")

run_every = f"{refresh_seconds}s" if auto_refresh else None


def state_label(value: str) -> str:
    return {
        "online": "Đang hoạt động",
        "stale": "Mất tín hiệu",
        "offline": "Offline",
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
    except DashboardDataError as exc:
        st.error(f"Không thể cập nhật dữ liệu: {exc}")
        return

    active_count = int((devices["observed_state"] == "online").sum())
    latest = overview.latest_received_at or "Chưa có dữ liệu"
    st.markdown(
        f'<span class="live-dot"></span><b>Live</b> '
        f'<span class="muted">· Bản tin mới nhất: {latest}</span>',
        unsafe_allow_html=True,
    )

    metrics = st.columns(6)
    metrics[0].metric("Thiết bị hoạt động", f"{active_count}/{overview.device_count}")
    metrics[1].metric("Tổng telemetry", f"{overview.telemetry_count:,}")
    metrics[2].metric(
        "Tốc độ 60 giây",
        f"{overview.messages_per_second_last_minute:.2f} msg/s",
        help=f"{overview.messages_last_minute} telemetry trong 60 giây gần nhất",
    )
    metrics[3].metric("Payload lỗi", f"{overview.invalid_payload_count:,}")
    metrics[4].metric(
        "Kịch bản bất thường",
        f"{overview.abnormal_scenario_runs:,}",
        help="Ground truth từ K2-K4, không phải kết quả Machine Learning",
    )
    metrics[5].metric(
        "Cảnh báo AI",
        "Chưa có" if not repository.has_detection_table() else "Có dữ liệu",
        help="Chỉ có sau khi Phase 3 tạo kết quả inference thực",
    )

    overview_tab, devices_tab, scenarios_tab, ai_tab = st.tabs(
        ["Tổng quan", "Thiết bị", "Kịch bản thử nghiệm", "AI & Cảnh báo"]
    )

    with overview_tab:
        left, right = st.columns([1.75, 1])
        with left:
            st.subheader("Lưu lượng MQTT telemetry")
            if traffic.empty:
                st.info("Chưa có telemetry trong khoảng thời gian đã chọn.")
            else:
                st.line_chart(
                    traffic,
                    x="bucket_at",
                    y="messages",
                    x_label="Thời gian (UTC)",
                    y_label="Số bản tin / bucket",
                    height=330,
                )
        with right:
            st.subheader("Phân bố nhãn thực nghiệm")
            st.caption("Nhãn từ scenario controller; không đi vào đầu vào ML.")
            if labels.empty:
                st.info("Chưa có nhãn trong khoảng thời gian đã chọn.")
            else:
                st.bar_chart(
                    labels,
                    x="ground_truth_label",
                    y="messages",
                    x_label="Ground truth",
                    y_label="Telemetry",
                    height=300,
                )

        st.subheader("Trạng thái thiết bị quan sát được")
        if devices.empty:
            st.info("Collector chưa ghi nhận thiết bị nào.")
        else:
            display_devices = devices.copy()
            display_devices["observed_state"] = display_devices["observed_state"].map(
                state_label
            )
            display_devices = display_devices.rename(
                columns={
                    "device_id": "Thiết bị",
                    "observed_state": "Trạng thái",
                    "age_seconds": "Độ trễ (s)",
                    "telemetry_in_window": "Telemetry",
                    "avg_payload_bytes": "Payload TB (bytes)",
                    "avg_inter_arrival_seconds": "Khoảng gửi TB (s)",
                    "last_sequence": "Sequence cuối",
                    "last_seen_at": "Lần cuối quan sát",
                }
            )
            st.dataframe(
                display_devices[
                    [
                        "Thiết bị",
                        "Trạng thái",
                        "Độ trễ (s)",
                        "Telemetry",
                        "Payload TB (bytes)",
                        "Khoảng gửi TB (s)",
                        "Sequence cuối",
                        "Lần cuối quan sát",
                    ]
                ],
                hide_index=True,
                width="stretch",
                height=390,
            )

    with devices_tab:
        if selected_device is None:
            st.info("Chọn một thiết bị ở thanh bên để xem phân tích chi tiết.")
            latest_messages = repository.latest_observations(limit=100)
            st.subheader("100 bản tin mới nhất")
            st.dataframe(
                latest_messages,
                hide_index=True,
                width="stretch",
                height=520,
            )
        else:
            history = repository.device_telemetry(selected_device)
            device_row = devices.loc[devices["device_id"] == selected_device]
            st.subheader(f"Chi tiết {selected_device}")
            if not device_row.empty:
                row = device_row.iloc[0]
                cols = st.columns(4)
                cols[0].metric("Trạng thái", state_label(str(row["observed_state"])))
                cols[1].metric("Sequence cuối", int(numeric_or_zero(row["last_sequence"])))
                cols[2].metric(
                    "Payload trung bình",
                    f"{numeric_or_zero(row['avg_payload_bytes']):.1f} bytes",
                )
                cols[3].metric(
                    "Khoảng gửi trung bình",
                    f"{numeric_or_zero(row['avg_inter_arrival_seconds']):.3f} s",
                )
            if history.empty:
                st.info("Thiết bị chưa có telemetry hợp lệ.")
            else:
                chart_left, chart_right = st.columns(2)
                with chart_left:
                    st.markdown("**Kích thước payload**")
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
                    st.markdown("**Nhiệt độ mô phỏng**")
                    st.line_chart(
                        history, x="received_at", y="temperature_c", height=250
                    )
                with sensor_right:
                    st.markdown("**Độ ẩm mô phỏng**")
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
        st.subheader("Lịch sử K1-K4")
        st.caption(
            "Mỗi run có request ID/run ID trong database và thời điểm bắt đầu/kết thúc thực."
        )
        runs = repository.scenario_runs()
        if runs.empty:
            st.info("Chưa chạy kịch bản thử nghiệm nào.")
        else:
            scenario_summary = (
                runs.groupby("scenario", as_index=False)
                .size()
                .rename(columns={"scenario": "Kịch bản", "size": "Số lần chạy"})
            )
            st.bar_chart(
                scenario_summary,
                x="Kịch bản",
                y="Số lần chạy",
                height=260,
            )
            display_runs = runs.rename(
                columns={
                    "device_id": "Thiết bị",
                    "scenario": "Kịch bản",
                    "started_at": "Bắt đầu",
                    "ended_at": "Kết thúc",
                    "configured_duration_seconds": "Cấu hình (s)",
                    "actual_duration_seconds": "Thực tế (s)",
                    "seed": "Seed",
                    "run_state": "Trạng thái",
                    "scenario_run_id": "Run ID",
                }
            )
            st.dataframe(
                display_runs[
                    [
                        "Thiết bị",
                        "Kịch bản",
                        "Trạng thái",
                        "Bắt đầu",
                        "Kết thúc",
                        "Cấu hình (s)",
                        "Thực tế (s)",
                        "Seed",
                        "Run ID",
                    ]
                ],
                hide_index=True,
                width="stretch",
                height=420,
            )

    with ai_tab:
        st.subheader("Isolation Forest & cảnh báo")
        if repository.has_detection_table():
            try:
                detections = repository.detections()
            except DashboardDataError as exc:
                st.error(f"Bảng detection không đúng contract: {exc}")
            else:
                anomaly_count = int(detections["is_anomaly"].fillna(0).astype(int).sum())
                col1, col2 = st.columns(2)
                col1.metric("Cảnh báo bất thường", anomaly_count)
                col2.metric("Detection records", len(detections))
                st.dataframe(
                    detections,
                    hide_index=True,
                    width="stretch",
                    height=480,
                )
        else:
            st.warning(
                "Chưa có bảng `anomaly_detections`: Isolation Forest và real-time "
                "inference thuộc Phase 3 chưa được triển khai. Dashboard không tạo "
                "điểm số hoặc cảnh báo giả."
            )
            st.markdown("**Ground truth bất thường hiện có để huấn luyện/đánh giá sau này**")
            abnormal_runs = repository.scenario_runs()
            abnormal_runs = abnormal_runs.loc[abnormal_runs["scenario"] != "normal"]
            if abnormal_runs.empty:
                st.info("Chưa có K2-K4 trong dataset.")
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
