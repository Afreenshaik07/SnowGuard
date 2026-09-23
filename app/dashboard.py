# ============================================================
# SNOWGUARD
# AI-POWERED DATA QUALITY & ANOMALY MONITORING PLATFORM
# ============================================================

import os
import sys
import subprocess
from pathlib import Path

import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv


# ============================================================
# ENVIRONMENT
# ============================================================

load_dotenv()


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="SnowGuard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# PROFESSIONAL THEME
# ============================================================

st.markdown(
    """
    <style>

    .stApp {
        background-color: #f6f8fc;
    }

    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1500px;
    }

    section[data-testid="stSidebar"] {
        background-color: #111827;
    }

    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] span {
        color: white;
    }

    div[data-testid="stMetric"] {
        background-color: white;
        border: 1px solid #e5e7eb;
        border-radius: 14px;
        padding: 18px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.03);
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 750;
    }

    .stButton button {
        border-radius: 9px;
        font-weight: 600;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ============================================================
# SNOWFLAKE CONNECTION
# ============================================================

@st.cache_resource
def create_connection():
    """
    Create reusable Snowflake connection.
    """

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="SNOWGUARD_DB",
        schema="ANALYTICS",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


# ============================================================
# QUERY HELPER
# ============================================================

def run_query(query):
    """
    Execute Snowflake query and return DataFrame.
    """

    connection = create_connection()
    cursor = connection.cursor()

    try:

        cursor.execute(query)

        rows = cursor.fetchall()

        columns = [
            column[0]
            for column in cursor.description
        ]

        return pd.DataFrame(
            rows,
            columns=columns,
        )

    finally:

        cursor.close()


# ============================================================
# PIPELINE RUNNER
# ============================================================

def run_pipeline_script(script_path):
    """
    Run one SnowGuard pipeline Python script.

    Windows-safe UTF-8 configuration is used because
    SnowGuard pipeline scripts contain Unicode characters
    such as check marks and emojis.
    """

    full_path = PROJECT_ROOT / script_path

    if not full_path.exists():

        raise FileNotFoundError(
            f"Pipeline script not found: {full_path}"
        )

    process_env = os.environ.copy()

    process_env["PYTHONIOENCODING"] = "utf-8"
    process_env["PYTHONUTF8"] = "1"

    result = subprocess.run(
        [sys.executable, str(full_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=process_env,
    )

    if result.returncode != 0:

        error_message = result.stderr.strip()

        if not error_message:
            error_message = result.stdout.strip()

        raise RuntimeError(
            error_message
            or f"Pipeline failed: {script_path}"
        )

    return result.stdout


# ============================================================
# LOAD MONITORING SUMMARY
# ============================================================

@st.cache_data(ttl=30)
def load_monitoring_summary():

    query = """
        SELECT
            SUMMARY_ID,
            CREATED_AT,
            TOTAL_RECORDS,
            QUALITY_SCORE,
            QUALITY_STATUS,
            ANOMALY_COUNT,
            ANOMALY_RATE,
            DRIFTED_FEATURE_COUNT,
            DRIFT_STATUS,
            RCA_COUNT,
            OVERALL_STATUS
        FROM SNOWGUARD_DB.ANALYTICS.MONITORING_SUMMARY
        ORDER BY CREATED_AT DESC
        LIMIT 1
    """

    return run_query(query)


# ============================================================
# LOAD DRIFT RESULTS
# ============================================================

@st.cache_data(ttl=30)
def load_drift_results():

    query = """
        SELECT *
        FROM SNOWGUARD_DB.QUALITY.DRIFT_RESULTS
    """

    return run_query(query)


# ============================================================
# LOAD ANOMALY RESULTS
# ============================================================

@st.cache_data(ttl=30)
def load_anomaly_results():

    query = """
        SELECT *
        FROM SNOWGUARD_DB.QUALITY.ANOMALY_RESULTS
    """

    return run_query(query)


# ============================================================
# LOAD ROOT CAUSE RESULTS
# ============================================================

@st.cache_data(ttl=30)
def load_root_cause_results():

    query = """
        SELECT *
        FROM SNOWGUARD_DB.ANALYTICS.ROOT_CAUSE_RESULTS
    """

    return run_query(query)


# ============================================================
# LOAD ALERT RESULTS
# ============================================================

@st.cache_data(ttl=30)
def load_alert_results():

    query = """
        SELECT
            ALERT_ID,
            CREATED_AT,
            ALERT_LEVEL,
            ALERT_TYPE,
            FEATURE_NAME,
            ALERT_MESSAGE,
            RECOMMENDATION,
            STATUS
        FROM SNOWGUARD_DB.ANALYTICS.ALERT_RESULTS
        ORDER BY CREATED_AT DESC
    """

    return run_query(query)


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.title("🛡️ SnowGuard")

    st.caption(
        "AI-Powered Data Monitoring"
    )

    st.divider()

    st.subheader("Navigation")

    page = st.radio(
        "Go to",
        [
            "Overview",
            "⚡ Run Monitoring",
            "Data Quality",
            "Anomaly Detection",
            "Drift Detection",
            "Root Cause Analysis",
            "Alerts & Recommendations",
        ],
        label_visibility="collapsed",
    )

    st.divider()

    st.subheader("System")

    st.success(
        "Snowflake Connected"
    )

    if st.button(
        "🔄 Refresh Data",
        width="stretch",
    ):

        st.cache_data.clear()
        st.rerun()

    st.divider()

    st.caption("SNOWGUARD_DB")
    st.caption("Production Monitoring")


# ============================================================
# LOAD ALL DATA
# ============================================================

try:

    summary_df = load_monitoring_summary()

    drift_df = load_drift_results()

    anomaly_df = load_anomaly_results()

    rca_df = load_root_cause_results()

    alert_df = load_alert_results()

except Exception as error:

    st.error(
        f"Snowflake connection failed: {error}"
    )

    st.stop()


# ============================================================
# RUN MONITORING PAGE
# ============================================================

if page == "⚡ Run Monitoring":

    st.title(
        "⚡ Run Full Monitoring Pipeline"
    )

    st.caption(
        "Execute the complete SnowGuard monitoring workflow from one place."
    )

    st.divider()

    # --------------------------------------------------------
    # PIPELINE STEPS
    # --------------------------------------------------------

    st.subheader(
        "Monitoring Pipeline"
    )

    pipeline_steps = [
        (
            "1",
            "Data Quality",
            "Validate missing values, duplicates and invalid records.",
        ),
        (
            "2",
            "Anomaly Detection",
            "Detect statistically unusual transaction records.",
        ),
        (
            "3",
            "Generate Drift Data",
            "Prepare reference and current datasets for drift analysis.",
        ),
        (
            "4",
            "Drift Detection",
            "Compare reference and current distributions.",
        ),
        (
            "5",
            "Root Cause Analysis",
            "Identify dimensions contributing to detected drift.",
        ),
        (
            "6",
            "Monitoring Summary",
            "Generate the overall SnowGuard health summary.",
        ),
        (
            "7",
            "Alert Engine",
            "Generate alerts and recommended actions.",
        ),
    ]

    for number, name, description in pipeline_steps:

        col1, col2, col3 = st.columns(
            [0.6, 2, 5]
        )

        with col1:

            st.metric(
                "Step",
                number,
            )

        with col2:

            st.write(
                f"**{name}**"
            )

        with col3:

            st.caption(
                description
            )

    st.divider()

    # --------------------------------------------------------
    # EXECUTE PIPELINE
    # --------------------------------------------------------

    st.subheader(
        "🚀 Execute Pipeline"
    )

    st.write(
        "Click the button below to execute all seven monitoring stages in sequence."
    )

    run_pipeline = st.button(
        "🚀 RUN FULL MONITORING PIPELINE",
        width="stretch",
        type="primary",
    )

    if run_pipeline:

        progress = st.progress(0)

        status_placeholder = st.empty()

        pipeline_status = st.container()

        scripts = [
            (
                "Data Quality",
                "app/quality/quality_engine.py",
            ),
            (
                "Anomaly Detection",
                "app/anomaly/anomaly_detector.py",
            ),
            (
                "Generate Drift Data",
                "app/drift/generate_drift_data.py",
            ),
            (
                "Drift Detection",
                "app/drift/drift_detector.py",
            ),
            (
                "Root Cause Analysis",
                "app/analytics/root_cause.py",
            ),
            (
                "Monitoring Summary",
                "app/analytics/monitoring_summary.py",
            ),
            (
                "Alert Engine",
                "app/analytics/alert_engine.py",
            ),
        ]

        success_count = 0

        try:

            with pipeline_status:

                for index, (name, script) in enumerate(
                    scripts
                ):

                    status_placeholder.info(
                        f"⏳ Running **{name}**..."
                    )

                    run_pipeline_script(
                        script
                    )

                    success_count += 1

                    progress.progress(
                        int(
                            (
                                (index + 1)
                                / len(scripts)
                            )
                            * 100
                        )
                    )

                    st.success(
                        f"✅ {name} completed successfully."
                    )

            status_placeholder.success(
                "🎉 Full monitoring pipeline completed successfully!"
            )

            st.balloons()

            st.cache_data.clear()

            st.info(
                "Dashboard data has been refreshed. "
                "Use the navigation menu to view the latest results."
            )

        except Exception as error:

            progress.empty()

            status_placeholder.error(
                "❌ Monitoring pipeline stopped."
            )

            st.error(
                f"Pipeline error: {error}"
            )

            st.warning(
                f"{success_count} pipeline stages completed before the failure."
            )

    st.divider()

    # --------------------------------------------------------
    # PIPELINE ARCHITECTURE
    # --------------------------------------------------------

    st.subheader(
        "Pipeline Architecture"
    )

    st.write(
        """
        **Raw Transactions**
        → **Data Quality**
        → **Anomaly Detection**
        → **Drift Data Generation**
        → **Drift Detection**
        → **Root Cause Analysis**
        → **Monitoring Summary**
        → **Alert Engine**
        → **SnowGuard Dashboard**
        """
    )

    st.info(
        "💡 This page controls the existing SnowGuard Python pipeline. "
        "The dashboard reads the resulting monitoring data directly from Snowflake."
    )

    st.stop()


# ============================================================
# VALIDATE SUMMARY
# ============================================================

if summary_df.empty:

    st.warning(
        "No monitoring summary available."
    )

    st.code(
        "python app\\analytics\\monitoring_summary.py",
        language="powershell",
    )

    st.stop()


# ============================================================
# EXTRACT MONITORING VALUES
# ============================================================

summary = summary_df.iloc[0]

total_records = int(
    summary["TOTAL_RECORDS"]
)

quality_score = float(
    summary["QUALITY_SCORE"]
)

quality_status = str(
    summary["QUALITY_STATUS"]
).upper()

anomaly_count = int(
    summary["ANOMALY_COUNT"]
)

anomaly_rate = float(
    summary["ANOMALY_RATE"]
)

drifted_feature_count = int(
    summary["DRIFTED_FEATURE_COUNT"]
)

drift_status = str(
    summary["DRIFT_STATUS"]
).upper()

rca_count = int(
    summary["RCA_COUNT"]
)

overall_status = str(
    summary["OVERALL_STATUS"]
).upper()

created_at = summary["CREATED_AT"]


# ============================================================
# ALERT COUNTS
# ============================================================

if alert_df.empty:

    total_alerts = 0
    critical_alerts = 0
    warning_alerts = 0
    open_alerts = 0

else:

    total_alerts = len(alert_df)

    critical_alerts = len(
        alert_df[
            alert_df["ALERT_LEVEL"]
            .astype(str)
            .str.upper()
            .eq("CRITICAL")
        ]
    )

    warning_alerts = len(
        alert_df[
            alert_df["ALERT_LEVEL"]
            .astype(str)
            .str.upper()
            .eq("WARNING")
        ]
    )

    open_alerts = len(
        alert_df[
            alert_df["STATUS"]
            .astype(str)
            .str.upper()
            .eq("OPEN")
        ]
    )


# ============================================================
# HEADER
# ============================================================

header_col1, header_col2 = st.columns(
    [5, 1]
)

with header_col1:

    st.title(
        "🛡️ SnowGuard"
    )

    st.caption(
        "AI-Powered Data Quality & Anomaly Detection Platform"
    )

with header_col2:

    st.caption(
        "Last monitoring run"
    )

    st.write(
        str(created_at)
    )

st.divider()


# ============================================================
# OVERVIEW
# ============================================================

if page == "Overview":

    st.header(
        "System Overview"
    )

    st.caption(
        "Real-time view of SnowGuard monitoring results"
    )

    # --------------------------------------------------------
    # KPI CARDS
    # --------------------------------------------------------

    col1, col2, col3, col4 = st.columns(
        4
    )

    with col1:

        st.metric(
            "📊 Data Quality",
            f"{quality_score:.2f}%",
            quality_status,
        )

    with col2:

        st.metric(
            "🚨 Anomalies",
            f"{anomaly_count:,}",
            f"{anomaly_rate:.2f}%",
            delta_color="inverse",
        )

    with col3:

        st.metric(
            "📈 Drifted Features",
            str(drifted_feature_count),
            drift_status,
            delta_color="inverse",
        )

    with col4:

        st.metric(
            "🔎 Root Causes",
            f"{rca_count:,}",
            "RCA records",
        )

    st.write("")

    # --------------------------------------------------------
    # SYSTEM HEALTH
    # --------------------------------------------------------

    st.subheader(
        "🛡️ Overall System Health"
    )

    if overall_status == "GOOD":

        st.success(
            f"🟢 SYSTEM STATUS: {overall_status}"
        )

    elif overall_status == "WARNING":

        st.warning(
            f"🟡 SYSTEM STATUS: {overall_status}"
        )

    else:

        st.error(
            f"🔴 SYSTEM STATUS: {overall_status}"
        )

    health_col1, health_col2, health_col3 = st.columns(
        3
    )

    with health_col1:

        st.metric(
            "Quality",
            quality_status,
        )

    with health_col2:

        st.metric(
            "Drift",
            drift_status,
        )

    with health_col3:

        st.metric(
            "Anomaly Rate",
            f"{anomaly_rate:.2f}%",
        )

    # --------------------------------------------------------
    # DATASET
    # --------------------------------------------------------

    st.subheader(
        "Dataset"
    )

    st.metric(
        "Total Transaction Records",
        f"{total_records:,}",
    )

    # --------------------------------------------------------
    # ALERT SUMMARY
    # --------------------------------------------------------

    st.subheader(
        "🚨 Alert Summary"
    )

    alert_col1, alert_col2, alert_col3, alert_col4 = st.columns(
        4
    )

    with alert_col1:

        st.metric(
            "Total Alerts",
            total_alerts,
        )

    with alert_col2:

        st.metric(
            "Critical",
            critical_alerts,
        )

    with alert_col3:

        st.metric(
            "Warnings",
            warning_alerts,
        )

    with alert_col4:

        st.metric(
            "Open Alerts",
            open_alerts,
        )

    # ========================================================
    # NEW VISUALIZATION SECTION
    # ========================================================

    st.subheader(
        "📊 Monitoring Visualizations"
    )

    # --------------------------------------------------------
    # DRIFT + ALERT CHARTS
    # --------------------------------------------------------

    chart_col1, chart_col2 = st.columns(
        2
    )

    # --------------------------------------------------------
    # DRIFT SEVERITY
    # --------------------------------------------------------

    with chart_col1:

        st.write(
            "**📈 Drift Severity Distribution**"
        )

        if (
            not drift_df.empty
            and "DRIFT_STATUS" in drift_df.columns
        ):

            drift_chart = (
                drift_df["DRIFT_STATUS"]
                .astype(str)
                .str.upper()
                .str.strip()
                .value_counts()
                .rename_axis("Status")
                .to_frame("Features")
            )

            st.bar_chart(
                drift_chart,
                width="stretch",
            )

        else:

            st.info(
                "No drift visualization available."
            )

    # --------------------------------------------------------
    # ALERT SEVERITY
    # --------------------------------------------------------

    with chart_col2:

        st.write(
            "**🚨 Alert Severity Distribution**"
        )

        if (
            not alert_df.empty
            and "ALERT_LEVEL" in alert_df.columns
        ):

            alert_chart = (
                alert_df["ALERT_LEVEL"]
                .astype(str)
                .str.upper()
                .str.strip()
                .value_counts()
                .rename_axis("Severity")
                .to_frame("Alerts")
            )

            st.bar_chart(
                alert_chart,
                width="stretch",
            )

        else:

            st.info(
                "No alert visualization available."
            )

    # --------------------------------------------------------
    # ANOMALY OVERVIEW
    # --------------------------------------------------------

    st.write("")

    st.subheader(
        "🚨 Anomaly Overview"
    )

    normal_count = max(
        total_records - anomaly_count,
        0,
    )

    anomaly_chart = pd.DataFrame(
        {
            "Records": [
                normal_count,
                anomaly_count,
            ]
        },
        index=[
            "Normal",
            "Anomalous",
        ],
    )

    st.bar_chart(
        anomaly_chart,
        width="stretch",
    )

    # --------------------------------------------------------
    # RCA FEATURE DISTRIBUTION
    # --------------------------------------------------------

    st.subheader(
        "🔎 Root Cause Analysis Overview"
    )

    if (
        not rca_df.empty
        and "FEATURE_NAME" in rca_df.columns
    ):

        rca_chart = (
            rca_df["FEATURE_NAME"]
            .astype(str)
            .str.upper()
            .str.strip()
            .value_counts()
            .rename_axis("Feature")
            .to_frame("RCA Records")
        )

        st.bar_chart(
            rca_chart,
            width="stretch",
        )

    else:

        st.info(
            "No RCA visualization available."
        )

    # --------------------------------------------------------
    # MONITORING INSIGHTS
    # --------------------------------------------------------

    st.subheader(
        "💡 Monitoring Insights"
    )

    insight1, insight2 = st.columns(
        2
    )

    with insight1:

        st.info(
            f"""
**Data Quality**

The monitored dataset contains
**{total_records:,} records** with an overall
quality score of **{quality_score:.2f}%**.

Current quality status:
**{quality_status}**
"""
        )

    with insight2:

        if drift_status == "HIGH":

            st.error(
                f"""
**Drift Alert**

**{drifted_feature_count} features** show
significant distribution changes.

Current drift severity:
**{drift_status}**
"""
            )

        else:

            st.info(
                f"""
**Drift Monitoring**

Drifted features:
**{drifted_feature_count}**

Current status:
**{drift_status}**
"""
            )


# ============================================================
# DATA QUALITY
# ============================================================

elif page == "Data Quality":

    st.header(
        "📊 Data Quality"
    )

    st.caption(
        "Overall quality health of the transaction dataset"
    )

    col1, col2 = st.columns(
        2
    )

    with col1:

        st.metric(
            "Quality Score",
            f"{quality_score:.2f}%",
        )

    with col2:

        st.metric(
            "Quality Status",
            quality_status,
        )

    st.progress(
        min(
            quality_score / 100,
            1.0,
        )
    )

    if quality_status == "GOOD":

        st.success(
            "Dataset quality is currently GOOD."
        )

    elif quality_status == "WARNING":

        st.warning(
            "Dataset quality requires attention."
        )

    else:

        st.error(
            "Dataset quality is CRITICAL."
        )

    st.subheader(
        "Dataset Statistics"
    )

    quality_col1, quality_col2 = st.columns(
        2
    )

    with quality_col1:

        st.metric(
            "Records",
            f"{total_records:,}",
        )

    with quality_col2:

        st.metric(
            "Anomaly Rate",
            f"{anomaly_rate:.2f}%",
        )


# ============================================================
# ANOMALY DETECTION
# ============================================================

elif page == "Anomaly Detection":

    st.header(
        "🚨 Anomaly Detection"
    )

    st.caption(
        "Records identified as statistically unusual"
    )

    col1, col2 = st.columns(
        2
    )

    with col1:

        st.metric(
            "Anomalies",
            f"{anomaly_count:,}",
        )

    with col2:

        st.metric(
            "Anomaly Rate",
            f"{anomaly_rate:.2f}%",
        )

    st.divider()

    if anomaly_df.empty:

        st.success(
            "No anomalies detected."
        )

    else:

        st.subheader(
            "Detected Anomalous Records"
        )

        st.dataframe(
            anomaly_df,
            width="stretch",
            hide_index=True,
        )


# ============================================================
# DRIFT DETECTION
# ============================================================

elif page == "Drift Detection":

    st.header(
        "📈 Drift Detection"
    )

    st.caption(
        "Statistical comparison between reference and current data"
    )

    col1, col2 = st.columns(
        2
    )

    with col1:

        st.metric(
            "Drifted Features",
            str(drifted_feature_count),
        )

    with col2:

        st.metric(
            "Overall Drift",
            drift_status,
        )

    st.divider()

    if drift_df.empty:

        st.info(
            "No drift results available."
        )

    else:

        # --------------------------------------------------------
        # NORMALIZE COLUMN NAMES FOR VISUALIZATIONS
        # --------------------------------------------------------

        drift_display = drift_df.copy()

        feature_column = None
        for candidate in [
            "FEATURE_NAME",
            "FEATURE",
            "FEATURE_COLUMN",
        ]:
            if candidate in drift_display.columns:
                feature_column = candidate
                break

        ks_column = None
        for candidate in [
            "KS_STATISTIC",
            "KS_VALUE",
            "KS_STAT",
            "KS",
        ]:
            if candidate in drift_display.columns:
                ks_column = candidate
                break

        pvalue_column = None
        for candidate in [
            "P_VALUE",
            "PVALUE",
            "P_VALUE_KS",
        ]:
            if candidate in drift_display.columns:
                pvalue_column = candidate
                break

        status_column = None
        for candidate in [
            "DRIFT_STATUS",
            "STATUS",
            "SEVERITY",
        ]:
            if candidate in drift_display.columns:
                status_column = candidate
                break

        reference_column = None
        for candidate in [
            "REFERENCE_MEAN",
            "REFERENCE_MEAN_VALUE",
            "REF_MEAN",
            "REFERENCE",
        ]:
            if candidate in drift_display.columns:
                reference_column = candidate
                break

        current_column = None
        for candidate in [
            "CURRENT_MEAN",
            "CURRENT_MEAN_VALUE",
            "CURR_MEAN",
            "CURRENT",
        ]:
            if candidate in drift_display.columns:
                current_column = candidate
                break

        # --------------------------------------------------------
        # FEATURE SUMMARY
        # --------------------------------------------------------

        st.subheader(
            "Feature Drift Summary"
        )

        summary_view = pd.DataFrame()

        if feature_column:
            selected_columns = [feature_column]

            for column in [
                ks_column,
                pvalue_column,
                status_column,
            ]:
                if column and column not in selected_columns:
                    selected_columns.append(column)

            summary_view = drift_display[selected_columns].copy()

            rename_map = {
                feature_column: "Feature",
            }

            if ks_column:
                rename_map[ks_column] = "KS Statistic"
            if pvalue_column:
                rename_map[pvalue_column] = "P-Value"
            if status_column:
                rename_map[status_column] = "Drift Status"

            summary_view = summary_view.rename(
                columns=rename_map
            )

            if "KS Statistic" in summary_view.columns:
                summary_view["KS Statistic"] = pd.to_numeric(
                    summary_view["KS Statistic"],
                    errors="coerce",
                )
                summary_view["KS Statistic"] = summary_view[
                    "KS Statistic"
                ].round(4)

            if "P-Value" in summary_view.columns:
                summary_view["P-Value"] = pd.to_numeric(
                    summary_view["P-Value"],
                    errors="coerce",
                )

            if "Drift Status" in summary_view.columns:
                summary_view["Drift Status"] = (
                    summary_view["Drift Status"]
                    .astype(str)
                    .str.upper()
                    .str.strip()
                )

            st.dataframe(
                summary_view,
                width="stretch",
                hide_index=True,
            )

        else:

            st.dataframe(
                drift_display,
                width="stretch",
                hide_index=True,
            )

        # --------------------------------------------------------
        # VISUALIZATION ROW 1
        # --------------------------------------------------------

        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:

            st.subheader(
                "📊 Drift Severity Distribution"
            )

            if status_column:
                drift_counts = (
                    drift_display[status_column]
                    .astype(str)
                    .str.upper()
                    .str.strip()
                    .value_counts()
                    .rename_axis("Status")
                    .to_frame("Features")
                )

                st.bar_chart(
                    drift_counts,
                    width="stretch",
                )
            else:
                st.info(
                    "Drift status column not available."
                )

        with chart_col2:

            st.subheader(
                "📈 KS Statistic by Feature"
            )

            if feature_column and ks_column:

                ks_chart = drift_display[[
                    feature_column,
                    ks_column,
                ]].copy()

                ks_chart[ks_column] = pd.to_numeric(
                    ks_chart[ks_column],
                    errors="coerce",
                )

                ks_chart = ks_chart.dropna(
                    subset=[ks_column]
                )

                if not ks_chart.empty:

                    ks_chart = ks_chart.set_index(
                        feature_column
                    )

                    st.bar_chart(
                        ks_chart[ks_column],
                        width="stretch",
                    )

                    st.caption(
                        "Higher KS statistics indicate a larger "
                        "distribution difference between reference "
                        "and current data."
                    )

            else:
                st.info(
                    "KS statistic data is not available."
                )

        # --------------------------------------------------------
        # P-VALUE VISUALIZATION
        # --------------------------------------------------------

        st.subheader(
            "📉 P-Value by Feature"
        )

        if feature_column and pvalue_column:

            pvalue_chart = drift_display[[
                feature_column,
                pvalue_column,
            ]].copy()

            pvalue_chart[pvalue_column] = pd.to_numeric(
                pvalue_chart[pvalue_column],
                errors="coerce",
            )

            pvalue_chart = pvalue_chart.dropna(
                subset=[pvalue_column]
            )

            if not pvalue_chart.empty:

                pvalue_chart = pvalue_chart.set_index(
                    feature_column
                )

                st.bar_chart(
                    pvalue_chart[pvalue_column],
                    width="stretch",
                )

                st.caption(
                    "For the KS test, a p-value below 0.05 "
                    "indicates statistically significant evidence "
                    "of distribution change."
                )

        else:
            st.info(
                "P-value data is not available in the drift results."
            )

        # --------------------------------------------------------
        # REFERENCE VS CURRENT COMPARISON
        # --------------------------------------------------------

        if (
            feature_column
            and reference_column
            and current_column
        ):

            st.subheader(
                "📊 Reference vs Current Mean"
            )

            comparison = drift_display[[
                feature_column,
                reference_column,
                current_column,
            ]].copy()

            comparison[reference_column] = pd.to_numeric(
                comparison[reference_column],
                errors="coerce",
            )

            comparison[current_column] = pd.to_numeric(
                comparison[current_column],
                errors="coerce",
            )

            comparison = comparison.dropna(
                subset=[
                    reference_column,
                    current_column,
                ]
            )

            if not comparison.empty:

                comparison = comparison.set_index(
                    feature_column
                )

                comparison = comparison.rename(
                    columns={
                        reference_column: "Reference",
                        current_column: "Current",
                    }
                )

                st.bar_chart(
                    comparison[[
                        "Reference",
                        "Current",
                    ]],
                    width="stretch",
                )

        else:

            st.info(
                "Reference/current mean columns are not stored in "
                "DRIFT_RESULTS, so the distribution-level mean comparison "
                "is not available here."
            )

        # --------------------------------------------------------
        # FULL EXISTING DRIFT RESULTS
        # --------------------------------------------------------

        st.subheader(
            "🔍 Complete Drift Results"
        )

        st.dataframe(
            drift_df,
            width="stretch",
            hide_index=True,
        )


# ============================================================
# ROOT CAUSE ANALYSIS
# ============================================================

elif page == "Root Cause Analysis":

    st.header(
        "🔎 Root Cause Analysis"
    )

    st.caption(
        "Dimensions contributing to detected distribution changes"
    )

    # --------------------------------------------------------
    # EXISTING RCA KPI
    # --------------------------------------------------------

    st.metric(
        "RCA Records",
        f"{rca_count:,}",
    )

    st.divider()

    if rca_df.empty:

        st.info(
            "No root cause analysis results available."
        )

    else:

        rca_display = rca_df.copy()

        # --------------------------------------------------------
        # DYNAMIC COLUMN DETECTION
        # --------------------------------------------------------

        feature_column = next((
            c for c in [
                "FEATURE_NAME",
                "FEATURE",
            ]
            if c in rca_display.columns
        ), None)

        severity_column = next((
            c for c in [
                "SEVERITY",
                "RCA_LEVEL",
                "DRIFT_STATUS",
                "STATUS",
            ]
            if c in rca_display.columns
        ), None)

        contribution_column = next((
            c for c in [
                "CONTRIBUTION_PERCENT",
                "CONTRIBUTION",
                "CONTRIBUTION_PCT",
                "PERCENT_CONTRIBUTION",
            ]
            if c in rca_display.columns
        ), None)

        dimension_type_column = next((
            c for c in [
                "DIMENSION_TYPE",
                "DIMENSION",
                "CATEGORY_TYPE",
            ]
            if c in rca_display.columns
        ), None)

        dimension_value_column = next((
            c for c in [
                "DIMENSION_VALUE",
                "VALUE",
                "CATEGORY_VALUE",
            ]
            if c in rca_display.columns
        ), None)

        # --------------------------------------------------------
        # RCA SUMMARY METRICS
        # --------------------------------------------------------

        severity_counts = {}

        if severity_column:
            severity_series = (
                rca_display[severity_column]
                .astype(str)
                .str.upper()
                .str.strip()
            )
            severity_counts = severity_series.value_counts().to_dict()

        high_count = int(severity_counts.get("HIGH", 0))
        medium_count = int(severity_counts.get("MEDIUM", 0))
        low_count = int(severity_counts.get("LOW", 0))

        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

        with metric_col1:
            st.metric(
                "🔴 High",
                high_count,
            )

        with metric_col2:
            st.metric(
                "🟡 Medium",
                medium_count,
            )

        with metric_col3:
            st.metric(
                "🟢 Low",
                low_count,
            )

        with metric_col4:
            st.metric(
                "📋 Total RCA",
                len(rca_display),
            )

        st.divider()

        # --------------------------------------------------------
        # VISUALIZATION ROW
        # --------------------------------------------------------

        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:

            st.subheader(
                "📊 RCA Records by Feature"
            )

            if feature_column:
                feature_chart = (
                    rca_display[feature_column]
                    .astype(str)
                    .str.upper()
                    .str.strip()
                    .value_counts()
                    .rename_axis("Feature")
                    .to_frame("RCA Records")
                )

                st.bar_chart(
                    feature_chart,
                    width="stretch",
                )
            else:
                st.info(
                    "Feature information is not available."
                )

        with chart_col2:

            st.subheader(
                "🚨 RCA Severity Distribution"
            )

            if severity_column:
                severity_chart = (
                    rca_display[severity_column]
                    .astype(str)
                    .str.upper()
                    .str.strip()
                    .value_counts()
                    .rename_axis("Severity")
                    .to_frame("Records")
                )

                st.bar_chart(
                    severity_chart,
                    width="stretch",
                )
            else:
                st.info(
                    "Severity information is not available."
                )

        # --------------------------------------------------------
        # CONTRIBUTION CHART
        # --------------------------------------------------------

        if feature_column and contribution_column:

            st.subheader(
                "📈 Feature Contribution"
            )

            contribution_chart = rca_display[[
                feature_column,
                contribution_column,
            ]].copy()

            contribution_chart[contribution_column] = pd.to_numeric(
                contribution_chart[contribution_column],
                errors="coerce",
            )

            contribution_chart = contribution_chart.dropna(
                subset=[contribution_column]
            )

            if not contribution_chart.empty:
                contribution_chart = (
                    contribution_chart
                    .groupby(feature_column)[contribution_column]
                    .sum()
                    .sort_values(ascending=False)
                    .to_frame("Contribution")
                )

                st.bar_chart(
                    contribution_chart,
                    width="stretch",
                )

        # --------------------------------------------------------
        # DIMENSION BREAKDOWN
        # --------------------------------------------------------

        if dimension_type_column:

            st.subheader(
                "🎯 Root Cause Dimension Breakdown"
            )

            dimension_chart = (
                rca_display[dimension_type_column]
                .astype(str)
                .str.upper()
                .str.strip()
                .value_counts()
                .rename_axis("Dimension")
                .to_frame("RCA Records")
            )

            st.bar_chart(
                dimension_chart,
                width="stretch",
            )

        # --------------------------------------------------------
        # EXISTING ROOT CAUSE RESULTS TABLE
        # --------------------------------------------------------

        st.subheader(
            "Root Cause Results"
        )

        st.dataframe(
            rca_display,
            width="stretch",
            hide_index=True,
        )


# ============================================================
# ALERTS & RECOMMENDATIONS
# ============================================================

elif page == "Alerts & Recommendations":

    st.header(
        "🚨 Alerts & Recommendations"
    )

    st.caption(
        "Automatically generated monitoring alerts and recommended actions"
    )

    # --------------------------------------------------------
    # ALERT SUMMARY
    # --------------------------------------------------------

    col1, col2, col3, col4 = st.columns(
        4
    )

    with col1:

        st.metric(
            "Total Alerts",
            total_alerts,
        )

    with col2:

        st.metric(
            "🔴 Critical",
            critical_alerts,
        )

    with col3:

        st.metric(
            "🟡 Warnings",
            warning_alerts,
        )

    with col4:

        st.metric(
            "Open Alerts",
            open_alerts,
        )

    st.divider()

    # --------------------------------------------------------
    # NO ALERTS
    # --------------------------------------------------------

    if alert_df.empty:

        st.success(
            "🎉 No active alerts detected."
        )

    else:

        st.subheader(
            "Active Monitoring Alerts"
        )

        # ----------------------------------------------------
        # FILTERS
        # ----------------------------------------------------

        filter_col1, filter_col2 = st.columns(
            2
        )

        with filter_col1:

            selected_level = st.selectbox(
                "Filter by severity",
                [
                    "ALL",
                    "CRITICAL",
                    "WARNING",
                    "INFO",
                ],
            )

        with filter_col2:

            selected_status = st.selectbox(
                "Filter by status",
                [
                    "ALL",
                    "OPEN",
                    "RESOLVED",
                ],
            )

        st.write("")

        # ----------------------------------------------------
        # APPLY FILTERS
        # ----------------------------------------------------

        filtered_alerts = alert_df.copy()

        if selected_level != "ALL":

            filtered_alerts = filtered_alerts[
                filtered_alerts["ALERT_LEVEL"]
                .astype(str)
                .str.upper()
                .eq(selected_level)
            ]

        if selected_status != "ALL":

            filtered_alerts = filtered_alerts[
                filtered_alerts["STATUS"]
                .astype(str)
                .str.upper()
                .eq(selected_status)
            ]

        # ----------------------------------------------------
        # DISPLAY ALERTS
        # ----------------------------------------------------

        if filtered_alerts.empty:

            st.info(
                "No alerts match the selected filters."
            )

        else:

            for _, alert in filtered_alerts.iterrows():

                level = str(
                    alert["ALERT_LEVEL"]
                ).upper()

                alert_type = (
                    str(
                        alert["ALERT_TYPE"]
                    )
                    .replace(
                        "_",
                        " ",
                    )
                    .title()
                )

                feature_name = alert[
                    "FEATURE_NAME"
                ]

                message = str(
                    alert["ALERT_MESSAGE"]
                )

                recommendation = str(
                    alert["RECOMMENDATION"]
                )

                status = str(
                    alert["STATUS"]
                ).upper()

                created = str(
                    alert["CREATED_AT"]
                )

                # ------------------------------------------------
                # ALERT LEVEL
                # ------------------------------------------------

                if level == "CRITICAL":

                    st.error(
                        f"🔴 CRITICAL | {alert_type}"
                    )

                elif level == "WARNING":

                    st.warning(
                        f"🟡 WARNING | {alert_type}"
                    )

                else:

                    st.info(
                        f"🔵 INFO | {alert_type}"
                    )

                # ------------------------------------------------
                # ALERT DETAILS
                # ------------------------------------------------

                detail_col1, detail_col2 = st.columns(
                    2
                )

                with detail_col1:

                    st.write(
                        f"**Status:** {status}"
                    )

                with detail_col2:

                    st.write(
                        f"**Created:** {created}"
                    )

                # ------------------------------------------------
                # FEATURE
                # ------------------------------------------------

                if (
                    not pd.isna(feature_name)
                    and str(feature_name).strip()
                    not in [
                        "",
                        "None",
                        "nan",
                    ]
                ):

                    st.write(
                        f"**Feature:** {feature_name}"
                    )

                # ------------------------------------------------
                # ALERT MESSAGE
                # ------------------------------------------------

                st.write(
                    "**Alert**"
                )

                st.write(
                    message
                )

                # ------------------------------------------------
                # RECOMMENDATION
                # ------------------------------------------------

                st.info(
                    f"**Recommended Action:** "
                    f"{recommendation}"
                )

                st.divider()

        # ----------------------------------------------------
        # ALERT DATA TABLE
        # ----------------------------------------------------

        st.subheader(
            "Alert Records"
        )

        st.dataframe(
            filtered_alerts,
            width="stretch",
            hide_index=True,
        )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "🛡️ SnowGuard  •  "
    "AI-Powered Data Quality & Anomaly Detection Platform  •  "
    "Python + Streamlit + Snowflake"
)