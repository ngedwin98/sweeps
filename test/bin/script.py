import time
import sys
import json
import os.path as path

rf = sys.argv[1]

with open(path.join(rf,'params.json')) as param_file:
    params = json.load(param_file)
a = params['param']

print(a)
time.sleep(25)
