"""
# runners/TGLF.py

Defines the TGLFrunner class for running TGLF codes.

"""

# import numpy as np
import os
from .base import Runner
from parsers.SHINEparser import SHINEparser
import subprocess

import pandas as pd

from dask.distributed import print


class AscotSdccWorkflowRunner(Runner):
    """
    """

    def __init__(self, executable_path, imas_db_suffix, return_mode='all', *args, **kwargs):
        """
        """
        self.executable_path = executable_path
        self.imas_db_suffix = imas_db_suffix # can be anything to identify this run and will be appended to the imas 
        self.return_mode = return_mode
    def single_code_run(self, params: dict, run_dir: str, index:int, *args,**kwargs):
        """
        """
        # Define the command and arguments
        executable_dir = os.path.dirname(self.executable_path)
        os.chdir(executable_dir)
        print('SHINErunner: single code run in:', run_dir)
        print('SHINErunner- parameters:', params)

        cmd = [
            self.executable_path,
            f"{params['enbi']}",
            f"{params['nbar']}",
            f"{params['np']}",
            f"{params['hfactor']}",
            f"{index}",
            f"{self.imas_db_suffix}",
            f"{run_dir}"
        ]
        # Run the command
        with open(os.path.join(run_dir,'shine_workflow.out'), "a") as out_file, open(os.path.join(run_dir,'shine_workflow.err'), "a") as err_file:
            result = subprocess.run(
                cmd,
                stdout=out_file,
                stderr=err_file,
                text=True,
                check=False
            )
        # result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        # print("Script output:\n", result.stdout)
        # print("Error running script:\n", e.stderr)
        
        
        # with open(os.path.join(run_dir,'shine_workflow.out'), 'w') as file:
        #     file.write(result.stdout)

        # with open(os.path.join(run_dir,'shine_workflow.err'), 'w') as file:
        #     file.write(result.stderr)

        shine_inj1, shine_inj2, te0, teav = self.parser.read_output_file(os.path.join(run_dir, 'results.csv'))
        if self.return_mode == 'all':
            output = [index, te0, teav]
            if self.nbi_injector == 1:
                output = output + [shine_inj2, shine_inj1]
            if self.nbi_injector == 2:
                output = output + [shine_inj1, shine_inj2]
        if self.return_mode == 'inj1':
            output = [shine_inj1]
        if self.return_mode == 'inj2':
            output = [shine_inj2]
        
        
        params_list = [str(v) for k,v in params.items()]
        return_list = params_list + output
        return_list = [str(li) for li in return_list]
        print('runner returning', ','.join(return_list))
        print('params', params)
        print('output', output)
        return_list = [str(li) for li in return_list]
        return ','.join(return_list)