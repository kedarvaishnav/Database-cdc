"""Small Streamlit interface for the database CDC replay demo."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parent
SYNTHETIC_INPUT = PROJECT_ROOT / "src" / "output" / "cdc_logs.jsonl"
SYNTHETIC_OUTPUT = PROJECT_ROOT / "src" / "output" / "final_state.parquet"
STACKOVERFLOW_INPUT = PROJECT_ROOT / "data" / "stackoverflow_revisions.jsonl"
STACKOVERFLOW_OUTPUT = PROJECT_ROOT / "src" / "output" / "stackoverflow_final_state.parquet"

DATASETS = {
    "Synthetic customer CDC": {
        "input": SYNTHETIC_INPUT,
        "output": SYNTHETIC_OUTPUT,
        "description": "Generated customer events with inserts, updates, deletes, and out-of-order delivery.",
        "kind": "synthetic",
    },
    "Stack Overflow revisions": {
        "input": STACKOVERFLOW_INPUT,
        "output": STACKOVERFLOW_OUTPUT,
        "description": "A small public revision-history sample replayed as CDC events.",
        "kind": "stackoverflow",
    },
}


def _signature(path: Path) -> str:
    """Return a lightweight cache key for an input file or Parquet directory."""
    if path.is_file():
        stat = path.stat()
        return f"{stat.st_mtime_ns}:{stat.st_size}"
    if path.is_dir():
        parts = [
            (item.stat().st_mtime_ns, item.stat().st_size)
            for item in path.rglob("*")
            if item.is_file()
        ]
        if parts:
            return f"{max(mtime for mtime, _ in parts)}:{sum(size for _, size in parts)}"
    return "missing"


@st.cache_data(ttl=30, max_entries=8)
def _count_events(path: str, signature: str) -> int:
    del signature  # Included in the cache key so file changes invalidate the result.
    with open(path, "rb") as event_file:
        return sum(1 for line in event_file if line.strip())


@st.cache_data(ttl=30, max_entries=8)
def _event_preview(path: str, signature: str, limit: int = 30) -> pd.DataFrame:
    del signature
    events = []
    with open(path, encoding="utf-8") as event_file:
        for line in event_file:
            if line.strip():
                events.append(json.loads(line))
            if len(events) >= limit:
                break

    frame = pd.json_normalize(events, sep=".")
    if not frame.empty:
        frame.insert(0, "arrival_order", range(1, len(frame) + 1))
    return frame


@st.cache_data(ttl=30, max_entries=8)
def _load_final_state(path: str, signature: str) -> pd.DataFrame:
    del signature
    return pd.read_parquet(path)


def _pipeline_environment() -> dict[str, str]:
    env = os.environ.copy()
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if not env.get("JAVA_HOME"):
        for candidate in (
            "/usr/lib/jvm/java-11-openjdk-amd64",
            "/usr/lib/jvm/default-java",
            "/usr/local/openjdk-11",
        ):
            if Path(candidate).is_dir():
                env["JAVA_HOME"] = candidate
                break

    if env.get("JAVA_HOME"):
        env["PATH"] = f"{env['JAVA_HOME']}/bin:{env.get('PATH', '')}"
    return env


def _run_pipeline(dataset: dict[str, object]) -> tuple[bool, str]:
    """Run the selected source through the repository's existing Spark processor."""
    input_path = Path(dataset["input"])
    output_path = Path(dataset["output"])
    if dataset["kind"] == "synthetic":
        commands = [[sys.executable, str(PROJECT_ROOT / "src" / "generate_cdc_data.py")]]
    else:
        commands = []

    commands.append([sys.executable, str(PROJECT_ROOT / "src" / "cdc_processor.py")])
    env = _pipeline_environment()
    env["CDC_INPUT_PATH"] = str(input_path)
    env["CDC_OUTPUT_PATH"] = str(output_path)
    logs = []

    for command in commands:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=1800,
            check=False,
        )
        logs.append(f"$ {' '.join(command)}\n{result.stdout}{result.stderr}")
        if result.returncode != 0:
            return False, "\n".join(logs)

    _count_events.clear()
    _event_preview.clear()
    _load_final_state.clear()
    return True, "\n".join(logs)


st.set_page_config(
    page_title="Database CDC replay",
    page_icon=":material/database:",
    layout="wide",
)

st.title("Database CDC replay")
st.write(
    "See how out-of-order change events become one current record per ID. "
    "The same replay works with the generated customer schema and the "
    "Stack Overflow revision fields."
)

with st.sidebar:
    st.header("Demo controls")
    selected_name = st.selectbox(
        "Choose a dataset",
        list(DATASETS),
        key="dataset_select",
    )
    dataset = DATASETS[selected_name]
    st.caption(dataset["description"])
    run_clicked = st.button(
        "Run selected pipeline",
        type="primary",
        width="stretch",
        key="run_pipeline",
    )
    st.caption("The button runs the existing Python and Spark pipeline in this WSL project.")

if run_clicked:
    if not Path(dataset["input"]).is_file() and dataset["kind"] != "synthetic":
        st.session_state["pipeline_result"] = (
            False,
            "The Stack Overflow sample is missing. Run `python3 src/fetch_stackoverflow_revisions.py` in WSL first.",
        )
    else:
        try:
            with st.spinner("Running the selected CDC pipeline in WSL…"):
                st.session_state["pipeline_result"] = _run_pipeline(dataset)
        except subprocess.TimeoutExpired as exc:
            partial = exc.stdout or b""
            if isinstance(partial, bytes):
                partial = partial.decode("utf-8", errors="replace")
            st.session_state["pipeline_result"] = (
                False,
                f"Pipeline exceeded the 30-minute limit.\n{partial}",
            )
        except Exception as exc:  # Surface environment and launch errors in the UI.
            st.session_state["pipeline_result"] = (False, str(exc))

result = st.session_state.get("pipeline_result")
if result:
    succeeded, log_text = result
    if succeeded:
        st.success("Pipeline finished. The latest-state output is refreshed below.")
    else:
        st.error("Pipeline did not finish successfully. Review the log for the failing step.")
    with st.expander("Pipeline log", expanded=not succeeded):
        st.code(log_text[-20000:] or "No command output was produced.", language="text")

st.subheader("How the replay works")
st.markdown(
    "**JSONL event stream** → **Spark orders events by ID and timestamp** → "
    "**Parquet keeps the latest non-deleted state**"
)

input_path = Path(dataset["input"])
output_path = Path(dataset["output"])
input_exists = input_path.is_file()
output_exists = output_path.is_dir() or output_path.is_file()

raw_count = _count_events(str(input_path), _signature(input_path)) if input_exists else None
events = (
    _event_preview(str(input_path), _signature(input_path))
    if input_exists
    else pd.DataFrame()
)
final_state = (
    _load_final_state(str(output_path), _signature(output_path))
    if output_exists
    else pd.DataFrame()
)

input_ids = (
    int(events["record_id"].nunique())
    if not events.empty and "record_id" in events.columns
    else None
)
final_count = len(final_state) if output_exists else None
duplicate_count = (
    int(final_state["record_id"].duplicated().sum())
    if output_exists and "record_id" in final_state.columns
    else None
)

with st.container(horizontal=True):
    st.metric("Input events", f"{raw_count:,}" if raw_count is not None else "Not generated", border=True)
    st.metric("IDs in preview", f"{input_ids:,}" if input_ids is not None else "—", border=True)
    st.metric("Latest active records", f"{final_count:,}" if final_count is not None else "Run pipeline", border=True)
    st.metric("Duplicate IDs", f"{duplicate_count:,}" if duplicate_count is not None else "—", border=True)

if input_exists:
    with st.container(border=True):
        st.subheader("Incoming CDC events")
        st.caption(
            f"Showing the first {len(events):,} events in arrival order. "
            "Payload columns are discovered from the selected file."
        )
        st.dataframe(events, hide_index=True, alt="Incoming CDC event sample in arrival order")
else:
    st.info("No event file exists for this dataset yet. Run the selected pipeline to generate and process it.")

if output_exists:
    with st.container(border=True):
        st.subheader("Deduplicated latest state")
        if duplicate_count == 0:
            st.success("One row per record ID; no duplicate IDs found.")
        elif duplicate_count is not None:
            st.warning(f"The output contains {duplicate_count:,} duplicate record IDs.")
        st.caption("Columns follow this dataset's output schema, so datasets can expose different fields.")
        st.dataframe(
            final_state.sort_values("record_id") if "record_id" in final_state.columns else final_state,
            hide_index=True,
            alt="Latest non-deleted state for each CDC record",
        )
else:
    st.info("The final-state Parquet output will appear here after the pipeline runs.")

if not input_exists and dataset["kind"] == "stackoverflow":
    st.caption("To fetch the public sample first, run `python3 src/fetch_stackoverflow_revisions.py` from the WSL project directory.")
