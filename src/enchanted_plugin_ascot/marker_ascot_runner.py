from enchanted_surrogates.runners.base_runner import Runner

class MarkerAscotRunner(Runner):
    def __init__(self, marker_config_path, plamsa_path, bfield_path, options_path, *args, **kwargs):
    self.marker_config_path = marker_config_path
    self.plamsa_path = plamsa_path
    self.bfield_path = bfield_path
    self.options_path = options_path
    
    def single_code_run(self, params: dict, run_dir: str, *args,**kwargs):
        raise NotImplementedError("This function is not yet implemented.")