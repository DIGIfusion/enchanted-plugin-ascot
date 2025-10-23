
import os
from enchanted_surrogates.runners.base_runner import Runner
from enchanted_plugin_ascot.shine_parser import ShineParser
import subprocess
import shutil

from dask.distributed import print

class ShineRunner(Runner):
    """
    Runner for executing the bbnbi --> chease workflow on sdcc; workflow put together by Pietro Vincenzi from Consorzio RFX, Italy.
    Runner made by Daniel Jordan from VTT Finland. 
    """

    def __init__(self, executable_path, nbi_injector, imas_db_suffix, return_mode='all', *args, **kwargs):
        """
        """
        self.parser = ShineParser()
        self.base_parameters_path = kwargs.get('base_parameters_path')
        
        self.executable_path = executable_path
        self.nbi_injector = nbi_injector # value of 1 or 2
        self.imas_db_suffix = imas_db_suffix # can be anything to identify this run and will be appended to the imas 
        self.return_mode = return_mode
        self.run_bbnbi = kwargs.get('run_bbnbi', 1)
        self.pl_spec = kwargs.get('pl_spec', "DT")
        self.nbi_spec = kwargs.get('nbi_spec', "D")
        self.metis_output_shot = kwargs.get('metis_output_shot', 130120) # ask pietro / check executable
        self.imasdb_version = kwargs.get('imasdb_version', 3)
        self.do_clean = kwargs.get('do_clean', False)
        
    def single_code_run(self, params: dict, run_dir: str, *args,**kwargs):
        """
        """        
        # Define the command and arguments
        executable_dir = os.path.dirname(self.executable_path)
        os.chdir(executable_dir)
        print('SHINErunner: single code run in:', run_dir)
        print('SHINErunner- parameters:', params)
        self.parser.write_input_file(params=params, run_dir=run_dir, imas_db_suffix=self.imas_db_suffix, run_bbnbi=self.run_bbnbi, PL_SPEC=self.pl_spec, NBI_SPEC=self.nbi_spec, output_log_path=run_dir, results_path=run_dir)
        
        if not os.path.exists(os.path.join(run_dir, 'shine.config')):
            raise FileNotFoundError(f"FOR SOME REASON THE CONFIG FILE WAS NOT CREATED BY THE PARSER: {os.path.join(run_dir, 'shine.config')}")
        
        cmd = f"{self.executable_path} --input {os.path.join(run_dir, 'shine.config')}"
        
        cmd = [
            self.executable_path,
            "--input",
            os.path.join(run_dir, "shine.config")
        ]
        
        # cmd = [
        #     self.executable_path,
        #     f"{params['enbi']}",
        #     f"{params['nbar']}",
        #     f"{params['np']}",
        #     f"{params['hfactor']}",
        #     f"{index}",
        #     f"{self.imas_db_suffix}",
        #     f"{run_dir}"
        # ]
        # Run the command
        with open(os.path.join(run_dir,'shine_workflow.out'), "a") as out_file, open(os.path.join(run_dir,'shine_workflow.err'), "a") as err_file:
            result = subprocess.run(
                cmd,
                stdout=out_file,
                stderr=err_file,
                text=True,
                check=False
            )

        # if self.run_bbnbi:
        #     output_dir=f"/home/ITER/{os.environ.get('USER')}/public/imasdb/BBNBI_AI_{self.imas_db_suffix}/{self.imasdb_version}/{self.metis_output_shot}/{params['index']}"
        # else:
        #     output_dir=f"/home/ITER/{os.environ.get('USER')}/public/imasdb/CHEASE_AI_{self.imas_db_suffix}/{self.imasdb_version}/{self.metis_output_shot}/{params['index']}"
        
        # shutil.copy(os.path.join(output_dir, 'results.csv'), os.path.join(run_dir, 'results.csv'))
        
        shine_inj1, shine_inj2, te0, teav, success = self.parser.read_output_file(run_dir, check_success=True)
        
        output = {'success':success}    
        if self.return_mode == 'all':
            output['te0'] = te0
            output['teav'] = teav
            if self.nbi_injector == 1:
                output['output_shine_inj1'] = shine_inj1
                output['shine_inj2'] = shine_inj2            
            elif self.nbi_injector == 2:
                output['shine_inj1'] = shine_inj1
                output['output_shine_inj2'] = shine_inj2            
        
        if self.return_mode == 'inj1':
            output['output_shine_inj1'] = shine_inj1
        if self.return_mode == 'inj2':
            output['output_shine_inj2'] = shine_inj2            
        
        if self.do_clean:
            self.parser.clean(run_dir, imasdb_suffix=self.imas_db_suffix)
        
        return output
