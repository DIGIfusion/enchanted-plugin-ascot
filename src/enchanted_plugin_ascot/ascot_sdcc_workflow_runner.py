"""
"""

# import numpy as np
import os
from .base import Runner
import subprocess

import pandas as pd

from dask.distributed import print

import subprocess
import shlex
import time
from typing import Optional


from enchanted_plugin_ascot.shine_parser import ShineParser
from enchanted_plugin_ascot.shine_runner import ShineRunner


class AscotSdccWorkflowRunner(Runner):
    """
    """

    def __init__(self, executable_path, imas_db_suffix, return_mode='all', shine_runner_config, *args, **kwargs):
        """
        """
        self.executable_path = executable_path
        self.imas_db_path = kwargs.get('imas_db_path', '/scratch/project_2013233/enchanted_runs/imasdb')
        self.imas_db_suffix = imas_db_suffix # can be anything to identify this run and will be appended to the imas 
        self.return_mode = return_mode
        self.shine_parser = ShineParser()
        self.shine_runner = ShineRunner()
        self.sdcc_ssh_host = kwargs['sdcc_ssh_host']
        self.remote_workflow_folder = kwargs.get('remote_workflow_folder', '/home/ITER/pietrov/shared_work_AIML/version8_DTplasma_H_Dnbi')
        self.remote_workflow_script = kwargs.get('remote_workflow_script','workflow_AI.sh')
        self.remote_config_path = kwargs.get('remote_config_path', '/home/ITER/jordand/ascot_workflow_configs')
    def single_code_run(self, params: dict, run_dir: str, index:int, *args,**kwargs):
        """
        """
        # make the config file for the precurser code on sdcc, ie pietros workflow
        prerun_config = self.shine_parser.write_input_file(params=params, run_dir=None, imas_db_suffix=self.shine_runner.imas_db_suffix, run_bbnbi=self.shine_runner.run_bbnbi, PL_SPEC=self.shine_runner.pl_spec, NBI_SPEC=self.shine_runner.nbi_spec, output_log_path='DEFAULT', results_path='DEFAULT')
        
        # send the file to sdcc, to be cleaned later
        run_id = os.path.basename(run_dir)
        prerun_config_path = os.path.join(self.remote_config_path, run_id+'_shine_config')
        cmd = ["ssh", self.sdcc_ssh_host, f"cat > {prerun_config_path}"]
        subprocess.run(cmd, input=prerun_config.encode(), check=True)
        
        # run the command on SDCC
        jobid = self.submit_remote_sbatch(config_path=prerun_config_path)
        
        # wait for the job to finish
        self.wait_for_job_completion(jobid, timeout_minutes=30)
        
        # copy the needed output of precurser run from sdcc
        
        
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
    
    def run_ssh_command(self, cmd: str, timeout: Optional[float] = None) -> str:
        ssh_cmd = ["ssh", sdcc_ssh_host, cmd]
        res = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
        if res.returncode != 0:
            raise RuntimeError(f"ssh command failed: {' '.join(ssh_cmd)}\nstdout: {res.stdout.decode()}\nstderr: {res.stderr.decode()}")
        return res.stdout.decode().strip()

    def submit_remote_sbatch(self, config_path) -> str:
        # Build remote command safely
        remote_dir = shlex.quote(self.remote_workflow_folder)
        remote_script = shlex.quote(f"{self.remote_workflow_folder.rstrip('/')}/{self.remote_workflow_script}")
        # join and quote script args
        submit_cmd = f"cd {remote_dir} && sbatch --parsable {remote_script} --input {config_path}"
        out = run_ssh_command(sdcc_ssh_host, submit_cmd)
        if not out:
            raise RuntimeError("sbatch returned empty output while expecting a parsable job id")
        return out.splitlines()[0].strip()

    def wait_for_job_completion(self, jobid: str, poll_seconds: int = 3, verbose: bool = True, timeout_minutes: Optional[int] = None) -> None:
        start_time = time.time()
        while True:
            # Check whether job appears in squeue
            # Use squeue -j <jobid> -h to hide headers; exit code 0 and non-empty stdout indicates presence
            check_cmd = f"squeue -j {shlex.quote(jobid)} -h"
            try:
                out = run_ssh_command(sdcc_ssh_host, check_cmd)
            except RuntimeError as e:
                # squeue errors may occur if job finished and squeue no longer returns, treat as absent
                out = ""
            if out.strip() == "":
                if verbose:
                    print(f"[{time.strftime('%H:%M:%S')}] job {jobid} no longer in squeue (finished or unknown).")
                break
            # still present
            elapsed_min = int((time.time() - start_time) / 60)
            if verbose:
                print(f"[{time.strftime('%H:%M:%S')}] waiting for job {jobid} — elapsed: {elapsed_min} min")
            if timeout_minutes is not None and (time.time() - start_time) > timeout_minutes * 60:
                raise TimeoutError(f"Timeout waiting for job {jobid} after {timeout_minutes} minutes")
            time.sleep(poll_seconds)

# if __name__ == "__main__":
#     # Example usage: adapt these variables to your environment or parse from argv
#     SSH_CONFIG_HOST = "sdcc2"
#     REMOTE_WORKFLOW_FOLDER = "/home/ITER/pietrov/shared_work_AIML/version5_HDF5+STimit_daniel_NObbnbi/"
#     REMOTE_WORKFLOW_SCRIPT = "batch_workflow_AI_sun_PV_args.sh"
#     # example script args (replace with actual values)
#     enbi = "value_enbi"
#     nbar = "value_nbar"
#     np = "value_np"
#     hfactor = "value_hfactor"
#     run_out = "value_run_out"
#     output_dir_suffix = "value_suffix"

#     # submit
#     jobid = submit_remote_sbatch(
#         SSH_CONFIG_HOST,
#         REMOTE_WORKFLOW_FOLDER,
#         REMOTE_WORKFLOW_SCRIPT,
#         enbi, nbar, np, hfactor, run_out, output_dir_suffix
#     )
#     print(f"Submitted job {jobid}")

#     # wait (customize poll_seconds and optional timeout_minutes)
#     wait_for_job_completion(SSH_CONFIG_HOST, jobid, poll_seconds=3, verbose=True, timeout_minutes=None)
#     print(f"Job {jobid} has finished.")
