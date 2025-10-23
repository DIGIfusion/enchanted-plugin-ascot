import subprocess
import os
import getpass
import numpy as np
from typing import List
from copy import deepcopy
import numpy as np 
from time import sleep
import pandas as pd
import numpy as np
from dask.distributed import print
import shutil

class ShineParser():
    def __init__(self):
        None
    
    def write_input_file(self, params, run_dir, imas_db_suffix, run_bbnbi=1, output_log_path='DEFAULT', results_path='DEFAULT', PL_SPEC="DT", NBI_SPEC="H"):
        print('debug write input file, params', type(params), params)
        print('debug enbi in params', 'enbi' in params)
        print('debug params items', params.items())
        
        print('debug parser run_bbnbi', run_bbnbi)
        
        assert 'enbi' in params
        assert 'nbar' in params
        assert 'np' in params
        assert 'hfactor' in params
        assert 'index' in params
        bbnbi_n_markers = params.get('bbnbi_n_markers', 10000)
        print('debug out log path', output_log_path)
        shine_config = f'''
# Configuration file for workflow_AI.sh

# --------------------------------------------------------------------------------------------------
# Setting parameters for plasma and NBI
# --------------------------------------------------------------------------------------------------

# Plasma species (allowed values: H, D, DT)
PL_SPEC="{PL_SPEC}"

# Beam species (allowed values: H, D. But D beam only for D and DT plasma, not for H plasma)
NBI_SPEC="{NBI_SPEC}"

# Energy of NBI in keV
ENBI={params['enbi']}

# Line averaged electron density in 10^19 m^-3
NBAR={params['nbar']}

# Electron density peaking factor
NP={params['np']}

# H-mode enhancement factor
HFACTOR={params['hfactor']}

# --------------------------------------------------------------------------------------------------
# Setting parameters for codes and output
# --------------------------------------------------------------------------------------------------

# Run number for IDS imasdb saving
RUN_OUT=1

# Suffix for output folder
OUTPUT_SUFFIX={imas_db_suffix}

# folder path for script .out log files, if DEFAULT then saved into imasdb, same as output IDS (public/imasdb/METIS_AI_..., /CHEASE_AI_..., /BBNBI_AI_...)
OUT_LOG_PATH={output_log_path}

# folder path for results.txt and results.csv containing shine-through (generated with RUN_BBNBI=1). If DEFAULT, then saved into imasdb (public/imasdb/BBNBI_AI_...)
RESULTS_PATH={results_path}

# If run_BBNBI=1 --> METIS+CHEASE+BBNBI+ST_extraction, if run_BBNBI=0 only METIS+CHEASE
RUN_BBNBI={run_bbnbi}

# Run number for IDS saving
RUN_OUT={params['index']}

# BBNBI MC markers
MC_MARKERS={bbnbi_n_markers}

# End of configuration file
        '''
        with open(os.path.join(run_dir, 'shine.config'), 'w') as file:
            file.write(shine_config)
        
        
    def read_output_file(self, run_dir, timeout=60, check_success=False):
        df = None
        shine_inj1, shine_inj2 = np.nan, np.nan
        te0 = np.nan
        teav = np.nan
        i = 0
        while df is None and i<= timeout:
            files = os.listdir(run_dir)
            results_file = None
            for file in files:
                if 'results.csv' in file:
                    results_file = file
            if results_file:
                file_path = os.path.join(run_dir, results_file)
                df = pd.read_csv(file_path)
                print('got the df')
            else:
                print('no df, sleeping 1 sec')
                sleep(1)
                i+=1
                
        if df is not None:
            print('debug df', df)
            shine_inj1 = float(df.loc[df['Parameter'] == 'shine_inj1', 'Value'].values[0])
            shine_inj2 = float(df.loc[df['Parameter'] == 'shine_inj2', 'Value'].values[0])
            te0 = float(df.loc[df['Parameter'] == "te0[keV]", 'Value'].values[0])
            teav = float(df.loc[df['Parameter'] == "teav[keV]", 'Value'].values[0])
        else: 
            raise FileNotFoundError(f'...results.csv was not found in run_dir {run_dir} before timeout of {timeout} sec.')
                
        if self.check_too_much_shine_through(run_dir):
            shine_inj1 = 0.51
            shine_inj2 = 0.51
        
        def is_valid_number(x):
            return (
                isinstance(x, (int, float, np.integer, np.floating)) and
                not isinstance(x, bool) and
                not np.isnan(x)
            )
        
        if check_success:
            if all(is_valid_number(out) for out in [shine_inj1, shine_inj2, te0, teav]):
                success = True
            else:
                print('output is not valid number')
                for var, name in zip([shine_inj1, shine_inj2, te0, teav],['shine_inj1', 'shine_inj2', 'te0', 'teav']):
                    print(f"{name}: {var} | is valid number: {is_valid_number(var)} | type: {type(var)}")
                success = False
            return shine_inj1, shine_inj2, te0, teav, success
        else:
            return shine_inj1, shine_inj2, te0, teav
    
    def clean(self, run_dir, imasdb_suffix, imasdb_dir = None):
        """
        Removes uneeded files
        """
        del_files = ['gen.out','metis.out', 'model.out', 'results.txt', 'shine.out', 'shine_workflow.err', 'shine_workflow.out', 'shine.config',]
        
        for file in os.listdir(run_dir):
            for del_file in del_files:
                if del_file in file:
                    shutil.rmtree(os.path.join(run_dir), file)
        
        if imasdb_dir is None:
            imasdb_dir = f"/home/ITER/{getpass.getuser()}/public/imasdb/"
        
        for subdir in os.listdir(imasdb_dir):
            if imasdb_suffix in subdir:
                shutil.rmtree(os.path.join(imasdb_dir, subdir))
        
    
    def check_too_much_shine_through(self, run_dir):
        phrase = "Too much shine-through:"
        too_much = False
        file_path = os.path.join(run_dir, 'shine_workflow.err')
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    if phrase in line:
                        too_much = True
        except FileNotFoundError:
            print("The file was not found.")
        except Exception as e:
            print(f"An error occurred: {e}")
        return too_much
    
if __name__ == '__main__':
    parser = ShineParser()
    # run_dir = '/home/ITER/jordand/enchanted-surrogates/tests/SDCC_tests/out/shine_8_1st/06427ce5-dc20-40ee-8c77-f370bf30f308'
    # run_dir = '/home/ITER/jordand/enchanted-surrogates/tests/SDCC_tests/out/shine_large_2/0a3ce948-8691-44a0-ae36-a43b9c8d364c'
    # print(parser.check_too_much_shine_through(run_dir))
    
    run_dir = '/home/ITER/jordand/enchanted_plugins/data/example_shine/batch_0/ShineRunner-4fc8dd85-f337-411c-9e13-69843601df29'
    print(parser.read_output_file(run_dir, check_success=True))
    