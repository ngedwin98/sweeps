import os, os.path as path, shutil
import hashlib, json
import numpy
import itertools

from .sweep_utils import get_timestamp

def create_rfs(sim, sweep, **kwargs):
    sweep_file = path.join(sim,sweep)
    for rf, params in read_sweep(sweep_file):
        rf_path = path.join(sim,'rfs',rf)
        if not path.exists(rf_path):
            os.mkdir(rf_path)
            with open(path.join(rf_path,'params.json'), 'w+') as file:
                file.write(params)
    sweep = get_timestamp() + '.create.json'
    shutil.copyfile(sweep_file, path.join(sim,'history',sweep))

def delete_rfs(sim, sweep, **kwargs):
    sweep_file = path.join(sim,sweep)
    for rf,_ in read_sweep(sweep_file): #TODO: Check equality of params.json?
        rf_path = path.join(sim,'rfs',rf)
        if path.exists(rf_path):
            shutil.rmtree(rf_path)
    sweep = get_timestamp() + '.delete.json'
    shutil.copyfile(sweep_file, path.join(sim,'history',sweep))

def read_sweep(sweep_file):
    sweep = json.load(sweep_file)
    const_vars = dict()
    sweep_vars = []
    sweep_values = []
    for var in sweep:
        sweep_type = sweep[var]["sweep_type"]
        sweep_value = sweep[var]["value"]
        if sweep_type == "constant":
            const_vars[var] = sweep_value
        elif sweep_type == "manual":
            sweep_vars.append(var)
            sweep_values.append(sweep_value)
        elif sweep_type == "linspace":
            sweep_vars.append(var)
            sweep_values.append(numpy.linspace(*sweep_value))
        else:
            print("Warning: sweep variable " + var + " ignored.")
    for values in itertools.product(*sweep_values):
        params = dict(zip(sweep_vars,values)).update(const_vars)
        params = json.dumps(params, indent=4, sort_keys=True)
        rf = hashlib.md5(params.encode('utf-8')).hexdigest()[:16]
        yield (rf, params)
