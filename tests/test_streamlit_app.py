"""Headless smoke tests for the CDC Streamlit interface."""

from pathlib import Path

from streamlit.testing.v1 import AppTest


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def test_app_renders_and_switches_between_dynamic_datasets():
    app = AppTest.from_file(str(APP_PATH)).run()

    assert not app.exception
    assert app.title[0].value == "Database CDC replay"
    assert app.selectbox(key="dataset_select").value == "Synthetic customer CDC"

    app.selectbox(key="dataset_select").select("Stack Overflow revisions").run()

    assert not app.exception
    assert app.selectbox(key="dataset_select").value == "Stack Overflow revisions"
    assert any(
        "payload.revision_number" in frame.value.columns
        for frame in app.dataframe
    )
