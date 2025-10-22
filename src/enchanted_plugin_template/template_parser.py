"""Template parser module.
"""
from enchanted_surrogates.parsers.base_parser import Parser


class TemplateParser(Parser):
    """An I/O parser

    Attributes
    ----------
    None

    Methods
    -------
    write_input_file(params: dict, run_dir: str)
        Writes the input file.

    clean_output_files(run_dir: str)
        Removes unnecessary files.

    """

    def write_input_file(self, params: dict, run_dir: str):
        """
        Writes input file.

        Parameters
        ----------
        params : dict
            Dictionary containing input parameters.
        run_dir : str
            Path to the run directory.

        """
        return

    def clean_output_files(self, run_dir: str):
        """
        Removes unnecessary files.

        Parameters
        ----------
        run_dir : str
            Path to the run directory.
        """
        return
