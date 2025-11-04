"""
"""

# import numpy as np
import os, sys
import subprocess

import warnings


import numpy as np
import pandas as pd
import shutil

from dask.distributed import print
from enchanted_surrogates.runners.base_runner import Runner

import subprocess
import shlex
import time
from typing import Optional
from pathlib import Path

import logging


from enchanted_plugin_ascot.shine_parser import ShineParser
from enchanted_plugin_ascot.shine_runner import ShineRunner
from enchanted_plugin_ascot.ascot_sdcc_workflow_parser import AscotSdccWorkflowParser
from datetime import datetime

import time

'''
THIS REQUIRES imas-python from pypy to be installed and imas core, only available to those in iter member states. 
'''

class AscotSdccWorkflowRunner(Runner):
    """
    """

    def __init__(self, shine_runner_config, *args, **kwargs):
        """
        """
        self.shine_runner = ShineRunner(**shine_runner_config)
        self.shine_parser = ShineParser()
        self.parser = AscotSdccWorkflowParser()
        self.imas_db_path = kwargs.get('imas_db_path', '/scratch/project_2013233/enchanted_runs/imasdb')
        self.sdcc_ssh_host = kwargs['sdcc_ssh_host']
        self.remote_workflow_folder = kwargs.get('remote_workflow_folder', '/home/ITER/pietrov/shared_work_AIML/version8_DTplasma_H_Dnbi')
        self.remote_workflow_sbatch = kwargs.get('remote_workflow_sbatch','/home/ITER/pietrov/shared_work_AIML/v8_workflow_sbatch_passconfig.sh')
        self.remote_config_path = kwargs.get('remote_config_path', '/home/ITER/jordand/ascot_workflow_configs')
        self.remote_user = kwargs.get('remote_user', 'jordand')
        self.idb_version=kwargs.get('idb_version',3)
        self.scenario=kwargs.get('scenario','130121')
        self.base_ascot_input_file = kwargs.get('base_ascot_input_file','/scratch/project_2013233/testdaniel/ascot_input.h5')
        self.ascot_executable = kwargs.get('ascot_executable', '/scratch/project_2013233/testdaniel/ascot5_main')
        self.marker_quantity = kwargs.get('marker_quantity',10)
        self.do_clean = kwargs.get('do_clean', True)
        
    def single_code_run(self, params: dict, run_dir: str, *args,**kwargs):
        """
        """
        start = time.time()
        print(datetime.now(),'\nmake the config file for the precurser code on sdcc, ie pietros workflow\n', run_dir)
        prerun_config = self.shine_parser.write_input_file(params=params, run_dir=None, imas_db_suffix=self.shine_runner.imas_db_suffix, run_bbnbi=self.shine_runner.run_bbnbi, PL_SPEC=self.shine_runner.pl_spec, NBI_SPEC=self.shine_runner.nbi_spec, output_log_path='DEFAULT', results_path='DEFAULT')
        
        print(datetime.now(),'\nsend the file to sdcc, to be cleaned later\n', run_dir)
        run_id = os.path.basename(run_dir)
        prerun_config_path = os.path.join(self.remote_config_path, run_id+'_shine_config')
        cmd = ["ssh", self.sdcc_ssh_host, f"cat > {prerun_config_path}"]
        subprocess.run(cmd, input=prerun_config.encode(), check=True)
        
        print(datetime.now(),'\nrun the command on SDCC\n',run_dir)
        jobid = self.submit_remote_sbatch(config_path=prerun_config_path)
        
        print(datetime.now(),'\nwait for the job to finish.\n',run_dir)
        self.wait_for_job_completion(jobid, timeout_minutes=30)
        print(datetime.now(),'\nfinished\n',run_dir)
        
        print(datetime.now(),'\ncopy the needed output of precurser run from sdcc\n',run_dir)
        REMOTE_OUTPUT_DIR=f"/home/ITER/{self.remote_user}/public/imasdb/BBNBI_AI_{self.shine_runner.imas_db_suffix}/{self.idb_version}/{self.scenario}/{params['index']}"
        LOCAL_OUTPUT_DIR=F"{self.imas_db_path}/BBNBI_AI_{self.shine_runner.imas_db_suffix}/{self.idb_version}/{self.scenario}/{params['index']}"
        if not os.path.exists(LOCAL_OUTPUT_DIR):
            os.makedirs(LOCAL_OUTPUT_DIR)
        
        self.scp_and_verify(REMOTE_OUTPUT_DIR, LOCAL_OUTPUT_DIR, expected_files=['equilibrium.h5','core_profiles.h5','nbi.h5'])
        
        print(datetime.now(),'\ncopy base ascot_input.h5 file to output dir\n',run_dir)
        shutil.copy(self.base_ascot_input_file, LOCAL_OUTPUT_DIR)
        
        print(datetime.now(),'\nalter base file based on equilibrium etc taken from sdcc\n',run_dir)
        self.parser.write_input_h5_file(imas_ids_path=LOCAL_OUTPUT_DIR, marker_quantity=self.marker_quantity)
        prelude_interval = time.time()
        print(datetime.now(),'\nrun ASCOT\n',run_dir)
        input_output_file = os.path.join(LOCAL_OUTPUT_DIR,'ascot_input.h5')
        self.run_ascot(input_output_file)
        
        # Parse the output 
        lost_power = self.parser.read_output(input_output_file)
        end = time.time()
        
        output = {'sdcc_preruntime_min': np.round((prelude_interval-start)/60, 2), 'ascot_runtime_min': np.round((end-prelude_interval)/60,2), 'total_runtime_min':np.round((end-start)/60,2)}
        if np.isnan(lost_power):
            output['success'] = False
            output['lost_power_w'] = lost_power
        else:
            output['success'] = True
            output['lost_power_w'] = lost_power
        
        # clean
        if self.do_clean:
            shutil.rmtree(run_dir)
            
            shutil.rmtree(LOCAL_OUTPUT_DIR)
            
            proc = subprocess.run(["ssh", self.sdcc_ssh_host, "rm", "-r", REMOTE_OUTPUT_DIR], capture_output=True, text=True)
            print(proc.stdout, end="")
            if proc.stderr:
                print("REMOTE CLEAN BBNBI ERR:", proc.stderr, file=sys.stderr, end="")

            REMOTE_OUTPUT_DIR_metis=f"/home/ITER/{self.remote_user}/public/imasdb/METIS_AI_{self.shine_runner.imas_db_suffix}/{self.idb_version}/{self.scenario}/{params['index']}"
            proc = subprocess.run(["ssh", self.sdcc_ssh_host, "rm", "-r", REMOTE_OUTPUT_DIR_metis], capture_output=True, text=True)
            print(proc.stdout, end="")
            if proc.stderr:
                print("REMOTE CLEAN METIS ERR:", proc.stderr, file=sys.stderr, end="")

        return output
    

    
    def run_ssh_command(self, cmd: str, timeout: Optional[float] = None) -> str:
        ssh_cmd = ["ssh", self.sdcc_ssh_host, cmd]
        res = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
        if res.returncode != 0:
            raise RuntimeError(f"ssh command failed: {' '.join(ssh_cmd)}\nstdout: {res.stdout.decode()}\nstderr: {res.stderr.decode()}")
        return res.stdout.decode().strip()

    def submit_remote_sbatch(self, config_path) -> str:
        # Build remote command safely
        remote_dir = shlex.quote(self.remote_workflow_folder)
        remote_sbatch = shlex.quote(f"{self.remote_workflow_sbatch}")
        # join and quote script args
        submit_cmd = f"cd {remote_dir} && sbatch --parsable {remote_sbatch} {config_path}"
        print(datetime.now(),'\n submit command: ', submit_cmd)
        out = self.run_ssh_command(submit_cmd)
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
                out = self.run_ssh_command(check_cmd)
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

    def run_ascot(self, input_path: str) -> int:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)]
        )

        """
        Run: srun $ASCOT_EXECUTABLE --in=$INPUT_PATH
        Blocks until completion. Prints stdout and stderr. Returns process exit code.
        """
        cmd = ["srun", self.ascot_executable, f"--in={input_path}"]
        logging.info("Running command: %s", " ".join(shlex.quote(c) for c in cmd))

        # Run and capture output
        proc = subprocess.run(cmd, capture_output=True, text=True)

        # Print stdout if any
        if proc.stdout:
            logging.info("ASCOT STDOUT:\n%s", proc.stdout.rstrip())

        # Print stderr if any (errors from the program)
        if proc.stderr:
            # Make stderr CSV/line-safe by replacing newlines with literal \n when needed in logs
            safe_err = proc.stderr.rstrip()
            logging.error("ASCOT STDERR:\n%s", safe_err)

        if proc.returncode != 0:
            logging.error("Process exited with non-zero return code: %d", proc.returncode)
        else:
            logging.info("Process completed successfully with return code 0")

        return proc.returncode

    def scp_and_verify(self, remote_path, local_path, expected_files):
        # Step 1: Run SCP
        scp_cmd = [
            "scp", "-r", "-q",
            f"{self.sdcc_ssh_host}:{remote_path}/*",
            local_path
        ]
        result = subprocess.run(scp_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            warnings.warn(f"SCP exited with code {result.returncode}. stderr: {result.stderr.strip()}")

        # Step 2: Verify local files
        missing = []
        for fname in expected_files:
            full_path = os.path.join(local_path, fname)
            if not os.path.exists(full_path):
                missing.append(fname)

        if missing:
            raise FileNotFoundError(f"Missing files after SCP: {missing}")
        else:
            print(f"✅ All expected files copied successfully.\n from {remote_path}\n to {local_path}")

    def light_post_processing(self):
        if self.clean:
            LOCAL_OUTPUT_DIR_FORDEL=f"{self.imas_db_path}/BBNBI_AI_{self.shine_runner.imas_db_suffix}/"
            shutil.rmtree(LOCAL_OUTPUT_DIR_FORDEL)
            
            REMOTE_OUTPUT_DIR_FORDEL_bbnbi = f"/home/ITER/{self.remote_user}/public/imasdb/BBNBI_AI_{self.shine_runner.imas_db_suffix}/"
            REMOTE_OUTPUT_DIR_FORDEL_metis = f"/home/ITER/{self.remote_user}/public/imasdb/METIS_AI_{self.shine_runner.imas_db_suffix}/"
            proc = subprocess.run(["ssh", self.sdcc_ssh_host, "rm", "-r", REMOTE_OUTPUT_DIR_FORDEL_bbnbi], capture_output=True, text=True)
            print(proc.stdout, end="")
            if proc.stderr:
                print("REMOTE CLEAN BBNBI ERR:", proc.stderr, file=sys.stderr, end="")

            proc = subprocess.run(["ssh", self.sdcc_ssh_host, "rm", "-r", REMOTE_OUTPUT_DIR_FORDEL_metis], capture_output=True, text=True)
            print(proc.stdout, end="")
            if proc.stderr:
                print("REMOTE CLEAN METIS ERR:", proc.stderr, file=sys.stderr, end="")

        
# Example usage (adapt variables)
if __name__ == "__main__":
    SSH_CONFIG_HOST = "sdcc2"                      # host or ip
    REMOTE_USER = "jordand"                        # remote user, optional
    REMOTE_OUTPUT_DIR = "/home/ITER/jordand/public/imasdb/BBNBI_AI_suffix/3/130120/run001"
    LOCAL_imasdb_DIR = "/scratch/project_2013233/enchanted_runs/imasdb"
    output_dir_suffix = "suffix"
    idb_version = "3"
    scenario = "130120"
    run_out = "run001"

    LOCAL_OUTPUT_DIR = os.path.join(LOCAL_imasdb_DIR, f"BBNBI_AI_{output_dir_suffix}", idb_version, scenario, run_out)

    scp_pull_remote_dir_contents(
        ssh_host=SSH_CONFIG_HOST,
        remote_dir=REMOTE_OUTPUT_DIR,
        local_dir=LOCAL_OUTPUT_DIR,
        user=REMOTE_USER,
    )

    print(f"Copied contents of {REMOTE_OUTPUT_DIR} to {LOCAL_OUTPUT_DIR}")


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
