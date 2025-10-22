import os
import pytest
import importlib.metadata
from enchanted_surrogates import load_plugins


def test_plugin_integration():

    # Load all sampler plugins
    plugins = load_plugins()
    print(plugins)

    # Ensure our plugin was discovered
    assert "template_runner" in plugins, "template runner entry point not found"
    assert "template_parser" in plugins, "template parser entry point not found"


def test_template_runner_entry_point():
    # Get entry points for this group
    eps = importlib.metadata.entry_points(group="enchanted_surrogates.runners")

    # Should not be empty
    assert eps, "No entry points found for group enchanted_surrogates.runners"

    # Find the one named "template_runner"
    ep = next((ep for ep in eps if ep.name == "template_runner"), None)
    assert ep is not None, "template_runner entry point not found"

    # Load the class from the entry point
    runner_cls = ep.load()

    # Check it's the right class
    assert runner_cls.__name__ == "TemplateRunner"

    # Instantiate with dummy args
    runner = runner_cls(
        executable_path="dummy_executable",
        other_params={"other_param": "value"}
    )

    # Check attributes exist
    assert runner.executable_path == "dummy_executable"
    assert runner.other_param == "value"


def test_template_parser_entry_point():
    # Get entry points for this group
    eps = importlib.metadata.entry_points(group="enchanted_surrogates.parsers")

    # Should not be empty
    assert eps, "No entry points found for group enchanted_surrogates.parsers"

    # Find the one named "template_parser"
    ep = next((ep for ep in eps if ep.name == "template_parser"), None)
    assert ep is not None, "template_parser entry point not found"

    # Load the class from the entry point
    parser_cls = ep.load()

    # Check it's the right class
    assert parser_cls.__name__ == "TemplateParser"

    # Instantiate with dummy args
    parser = parser_cls()

    # Check methods exist and callable
    assert hasattr(parser, "write_input_file")
    assert hasattr(parser, "clean_output_files")

    # Call methods to confirm they don’t crash
    parser.write_input_file(params={"x": 1}, run_dir="dummy_dir")
    parser.clean_output_files(run_dir="dummy_dir")
