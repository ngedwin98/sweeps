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

def file_hash(script_path):
    with open(script_path) as script:
        contents = script.read()
        return hashlib.md5(contents.encode('utf-8')).hexdigest()

class Status(enum.Enum):
    NEW = 0
    FAILED = -1
    FINISHED = 1
    RUNNING = 2
    INVALID = 3

def generate_status(action, script_hash):
    return " | ".join((action, get_timestamp(), script_hash))

def check_status(rf, script_hash):
    with open(path.join(rf,"status.txt")) as file:
        status = Status.NEW
        for line in file:
            action, _, script = (s.strip() for s in line.split('|'))
            if script != script_hash:
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

def collect_rf_status(sim, script_hash):
    status_table = {e : set() for e in Status}
    rfs_path = path.join(sim,"rfs")
    for rf in os.listdir(rfs_path):
        status = check_status(path.join(rfs_path,rf), script_hash)
        status_table[status].add(rf)
    return status_table

# Create useful report of statuses
def generate_report():
    pass
