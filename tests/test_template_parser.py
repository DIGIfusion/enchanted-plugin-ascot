import pytest
from enchanted_plugin_template.template_parser import TemplateParser


def test_template_parser_instantiation():
    parser = TemplateParser()
    assert isinstance(parser, TemplateParser)


def test_write_input_file_runs_without_error(tmp_path):
    parser = TemplateParser()
    params = {"param1": 42}
    run_dir = tmp_path

    # Should not raise
    parser.write_input_file(params=params, run_dir=str(run_dir))


def test_clean_output_files_runs_without_error(tmp_path):
    parser = TemplateParser()
    run_dir = tmp_path

    # Should not raise
    parser.clean_output_files(run_dir=str(run_dir))
