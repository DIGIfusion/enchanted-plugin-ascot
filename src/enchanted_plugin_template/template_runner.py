import os
import subprocess
import numpy as np
from enchanted_surrogates.runners.base_runner import Runner
from .template_parser import TemplateParser


class TemplateRunner(Runner):
    """
    Class for running a template code.


    Attributes
    ----------
    executable_path : str
        the path to the pre-compiled executable binary

    See other_params.

    Methods
    -------
    single_code_run()
        Runs the code after processing the input.

    """

    def __init__(
        self,
        executable_path,
        other_params: dict,
        *args,
        **kwargs,
    ):
        """
        Initializes the runner object.

        Args:
            executable_path (str): The path to the pre-compiled executable binary.
            other_params (dict): Dictionary containing other parameters for initialization.

        other_params:
            other_param : str
                Description of the parameter.
            ...

        """
        self.parser = TemplateParser()
        self.executable_path = executable_path
        self.other_param = other_params["other_param"]

    def single_code_run(self, params: dict, run_dir: str):
        """
        Runs simulation.

        Args:
            params (dict): Dictionary containing parameters for the simulation.
            run_dir (str): Directory where the simulation code is run.
            tolerance (float): Tolerance for beta iteration

        Returns:
            bool: True if the simulation is successful, False otherwise.

        """
        # Initialize success flag
        success = False

        # Change to run_dir if needed
        # os.chdir(run_dir)

        # Call parser to write input file
        # self.parser.write_input_file()

        # Execute code, for example:
        # subprocess.call([self.executable_path])

        # Process output, for example
        # success = self.parser.write_summary(run_dir, params)
        success = True  # Placeholder for actual success condition

        # Cleanup files
        self.parser.clean_output_files(run_dir)

        return {'success': success}
