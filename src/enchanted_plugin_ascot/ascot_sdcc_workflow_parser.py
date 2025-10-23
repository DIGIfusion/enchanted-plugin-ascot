import os
import sys
import copy
import subprocess

import unyt
import numpy as np
from mpi4py import MPI

from a5py import Ascot
from a5py.ascot5io.options import Opt
import a5py.templates.imasinterface as imasinterface

from enchanted_plugin_ascot.shine_parser import ShineParser

class AscotSdccWorkflowParser(Parser):
    """
    """

    def write_input_h5_file(self, imas_ids_path):
        """
        Writes input file.

        Parameters
        ----------
        params : dict
            Dictionary containing input parameters.
        run_dir : str
            Path to the run directory.

        """
        rank, size = MPI.COMM_WORLD.Get_rank(), MPI.COMM_WORLD.Get_size()

        if rank == 0:
            nmrk = 10 #100
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
        else:
            raise ValueError(f"MPI.COMM_WORLD.Get_rank() returned {rank}. It needs to be 0")
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
