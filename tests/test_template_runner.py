# tests/test_template_runner.py

import pytest
from enchanted_plugin_template.template_runner import TemplateRunner


def test_init_sets_attributes():
    runner = TemplateRunner(
        executable_path="/path/to/exe",
        other_params={"other_param": "value"},
    )

    assert runner.executable_path == "/path/to/exe"
    assert runner.other_param == "value"


def test_single_code_run_success(monkeypatch, tmp_path):
    # Arrange
    run_dir = tmp_path
    params = {"alpha": 0.5}

    runner = TemplateRunner(
        executable_path="/fake/exe",
        other_params={"other_param": "foo"},
    )

    # Act
    result = runner.single_code_run(params=params, run_dir=str(run_dir))

    # Assert
    assert result == {"success": True}
