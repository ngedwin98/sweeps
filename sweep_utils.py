import datetime
import hashlib
import os
import os.path as path
import enum

def asheader(message, prefix="", length=80, line='-'):
    spaces = length - len(prefix) - len(message)
    n = spaces // 2
    return prefix + line*n + message + line*(spaces-n)

def get_timestamp():
    return datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

def write(file, message):
    file.flush()
    file.write(message+'\n')
    file.flush()

def get_script_id(script, sim):
    script_path = path.join(sim, "bin", script)
    with open(script_path) as file:
        contents = file.read()
        return script + "@" + hashlib.md5(contents.encode('utf-8')).hexdigest()

class Status(enum.Enum):
    NEW = 0
    FAILED = -1
    FINISHED = 1
    RUNNING = 2
    INVALID = 3

def generate_status(action, script_id):
    return " | ".join((action, get_timestamp(), script_id))

def check_status(rf, script, sim):
    with open(path.join(sim,'rfs',rf,'status.txt')) as file:
        status = Status.NEW
        for line in file:
            action, _, script_id = (s.strip() for s in line.split('|'))
            if script_id != get_script_id(script,sim):
                continue
            if status in (Status.NEW, Status.FAILED, Status.FINISHED):
                if action == "STARTED":
                    status = Status.RUNNING
                else:
                    status = Status.INVALID
            elif status is Status.RUNNING:
                if action == "FINISHED":
                    status = Status.FINISHED
                elif action == "FAILED":
                    status = Status.FAILED
                else:
                    status = Status.INVALID
    return status

def collect_rf_status(script, sim):
    status_table = {e : set() for e in Status}
    for rf in os.listdir(path.join(sim,'rfs')):
        status = check_status(rf, script, sim)
        status_table[status].add(rf)
    return status_table
