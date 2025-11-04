from enchanted_surrogates.utils.is_package_available import is_package_available
# check that needed packages are available
import importlib
import os
import sys
import copy
import subprocess
import unyt
import numpy as np



from enchanted_surrogates.utils.is_package_available import is_package_available
if is_package_available("dask.distributed"):
    from dask.distributed import print

_LIBS_LOADED = False

def _ensure_runtime_is_clean():
    # small sanity checks you can expand: ensure LD_LIBRARY_PATH doesn't contain pmix/openmpi
    ld = os.environ.get("LD_LIBRARY_PATH", "")
    if "pmix" in ld or "openmpi" in ld:
        print("Warning: LD_LIBRARY_PATH contains pmix/openmpi; this may break MPICH-linked libraries")
    # don't attempt to change env here

def _lazy_import_libs():
    global _LIBS_LOADED, _libascot, _Ascot
    if _LIBS_LOADED:
        return
    _ensure_runtime_is_clean()
    try:
        # now import ascot package (which loads libascot)
        from a5py import Ascot
        from a5py.ascot5io.options import Opt
        import a5py.templates.imasinterface as imasinterface

    except Exception as e:
        # surface diagnostic; do NOT let importlib swallow this silently
        print("Failed to import MPI-linked libraries at plugin load:", e)
        raise
    _LIBS_LOADED = True

# print("=== Dask Worker Environment ===")
# print("LD_LIBRARY_PATH:", os.environ.get("LD_LIBRARY_PATH"))
# print("PYTHONPATH:", os.environ.get("PYTHONPATH"))
# print("sys.path:", os.sys.path)

# print("\n=== libascot.so MPI linkage ===")
# mpi_output = subprocess.run(
#     ["ldd", "/projappl/project_2013233/ascot5/build/libascot.so"],
#     capture_output=True,
#     text=True
# )
# print(mpi_output.stdout)

# import subprocess
# result = subprocess.run(
#     ["ldd", "/projappl/project_2013233/ascot5/build/libascot.so"],
#     capture_output=True,
#     text=True
# )
# print("ldd output for libascot.so:\n", result.stdout)

# import csv

from enchanted_plugin_ascot.shine_parser import ShineParser
from enchanted_surrogates.parsers.base_parser import Parser
class AscotSdccWorkflowParser(Parser):
    """
    """
    def __init__(self):
        #imports are here so it is only imported when the object is made
        #This helps if the optinal dependancies are not installed. For example imasinterface
        from a5py import Ascot
        from a5py.ascot5io.options import Opt
        import a5py.templates.imasinterface as imasinterface

        
    def write_input_h5_file(self, imas_ids_path, marker_quantity):
        """
        Writes input file.

        Parameters
        ----------
        params : dict
            Dictionary containing input parameters.
        run_dir : str
            Path to the run directory.

        """
        nmrk = marker_quantity #100
        time = 324.0
        # case = sys.argv[2]
        # scenario = sys.argv[1]
        # path = f"/scratch/project_2013233/imasdb/{scenario}/{case}" # needs to be altered

        inputs = {}
        bfield, plasma = None, None

        # subprocess.run(["mkdir", "-p", scenario])
        # subprocess.run(["mkdir", "-p", scenario+"/"+case])
        # subprocess.run(["cp", "input.h5", scenario+"/"+case+"/input.h5"])

        a5 = Ascot(imas_ids_path+"/ascot_input.h5")
        equilibrium_ids = imasinterface.read_ids(
            "equilibrium", query=imas_ids_path, time_requested=time,
        )
        a5.data.create_input(
            "imas_b2ds", equilibrium_ids=equilibrium_ids, activate=True
        )
        bfield = a5.data.bfield.active.read()
        core_profiles_ids = imasinterface.read_ids(
            "core_profiles", query=imas_ids_path, time_requested=time
        )

        pls = a5.data.create_input(
            "imas_plasma",
            core_profiles_ids=core_profiles_ids,
            equilibrium_ids=equilibrium_ids,
            psi0=bfield["psi0"],
            psi1=bfield["psi1"],
            dryrun=True,
        )
        pls["mass"] *= unyt.amu
        pls["charge"] = pls["charge"].v

        pls_original = copy.deepcopy(pls)
        nion = 3
        pls["nion"] = nion
        pls["anum"] = pls["anum"][:nion]
        pls["znum"] = pls["znum"][:nion]
        pls["mass"] = pls["mass"][:nion]
        pls["charge"] = pls["charge"][:nion]
        pls["idensity"] = pls["idensity"][:,:nion]
        a5.data.create_input("plasma_1D", **pls, desc="BBNBI", activate=True)
        a5.data.create_input("plasma_1D", **pls_original, desc="ASCOT")

        nbi_ids = imasinterface.read_ids("nbi", query=imas_ids_path)
        a5.data.create_input("imas_nbi", nbi_ids=nbi_ids, activate=True)

        opt = Opt.get_default()

        a5.simulation_initoptions(**opt)
        a5.simulation_initbbnbi(**inputs)
        vrun = a5.simulation_bbnbi(nmrk)
        ids = vrun.getstate("ids", endcond="IONIZED")
        mrk = vrun.getstate_markers("gc", ids=ids)
        mrk["weight"] /= unyt.s
        a5.simulation_free()

        shinethrough = 1.0 - mrk["n"] / nmrk
        print(
            f"BBNBI generated {mrk['n']} markers "
            f"(ST: {int(100*shinethrough)}%)"
        )

        mrk["charge"][:] = 1
        a5.data.create_input("gc", **mrk, activate=True)

        opt.update({
            # Simulation mode
            "SIM_MODE":2, "ENABLE_ADAPTIVE":1,
            # End conds
            "ENDCOND_SIMTIMELIM":1, "ENDCOND_MAX_MILEAGE":1.5,
            "ENDCOND_CPUTIMELIM":1, "ENDCOND_MAX_CPUTIME":1000.0,
            "ENDCOND_ENERGYLIM":1, "ENDCOND_MIN_ENERGY":2.0e3, "ENDCOND_MIN_THERMAL":2.0,
            # Physics
            "ENABLE_ORBIT_FOLLOWING":1, "ENABLE_COULOMB_COLLISIONS":1,
            # Distribution output
            "ENABLE_DIST_5D":0,
            "DIST_MIN_R":4.3,        "DIST_MAX_R":8.3,       "DIST_NBIN_R":50,
            "DIST_MIN_PHI":0,        "DIST_MAX_PHI":360,     "DIST_NBIN_PHI":1,
            "DIST_MIN_Z":-2.0,       "DIST_MAX_Z":2.0,       "DIST_NBIN_Z":50,
            "DIST_MIN_PPA":-1.3e-19, "DIST_MAX_PPA":1.3e-19, "DIST_NBIN_PPA":100,
            "DIST_MIN_PPE":0,        "DIST_MAX_PPE":1.3e-19, "DIST_NBIN_PPE":50,
            "DIST_MIN_TIME":0,       "DIST_MAX_TIME":1.0,    "DIST_NBIN_TIME":1,
            "DIST_MIN_CHARGE":1,     "DIST_MAX_CHARGE":3,    "DIST_NBIN_CHARGE":1,
        })
        a5.data.create_input("opt", **opt, activate=True)
        a5.data.plasma.ASCOT.activate()
        return
    
    def write_input_file(args, kwargs):
        return write_input_h5_file(*args, **kwargs)

    def read_output(self, ascot_path_h5):
        lost_power = np.nan
        # case = sys.argv[2]
        # scenario = sys.argv[1]
        # path = scenario+"/"+case
        a5 = Ascot(ascot_path_h5)
        try:
            weight, ekin = a5.data.active.getstate(
                "weight", "ekin", state="end", endcond=["WALL", "NONE"]
            )
            lost_power = float(np.sum(weight * ekin).to("W").v if len(weight) else 0.0)
        except:
            lost_power = np.nan
        # print(f"Lost power is {lost_power} W.")
        # headers = ["Lost power [W]",]
        # rows = [(lost_power,),]
        # with open(path+"/slowingdown.csv", "w", newline="", encoding="utf-8") as f:
        #     writer = csv.writer(f)
        #     writer.writerow(headers)
        #     writer.writerows(rows)

        # print("Done")
        return lost_power
        
    def clean_output_files(self, run_dir: str):
        """
        Removes unnecessary files.

        Parameters
        ----------
        run_dir : str
            Path to the run directory.
        """
        return
