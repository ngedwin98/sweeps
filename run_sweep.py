import argparse
import os.path as path
import os
import datetime
import atexit, signal
import subprocess
import multiprocessing

from sweep_utils import write, file_hash, get_timestamp, asheader
from sweep_utils import Status, collect_rf_status, generate_status

__all__ = ["run_sweep"]

# script must be a relative path starting at sim/bin
# rfs specified in a file relative path from sim, should contain rf hashes
# relative to sim/rfs
def run_sweep(interp, sim, script, num_procs, rerun_failed=False, rfs=None):
    script_hash = script+"@"+file_hash(path.join(sim,"bin",script))
    timestamp = get_timestamp()
    # Determine status of all requested rfs
    rf_status = collect_rf_status(sim, script_hash)
    if rfs is not None:
        with open(path.join(sim,rfs)) as rfs_file:
            rfs = [l for l in rfs_file.readline() if not l.startswith('#')]
            for status in Status:
                rf_status[status].intersection_update(rfs)
    queued_rfs = set(rf_status[Status.NEW])
    if rerun_failed:
        queued_rfs.update(rf_status[Status.FAILED])
    # Write summary of rf statuses to file
    runfile = timestamp+'.run'
    with open(path.join(sim,runfile), 'a+') as run:
        write(run, "# RUN FILE FOR SWEEP GENERATED AT " + timestamp)
        write(run, "# script: " + script_hash)
        write(run, "# rerun_failed: " + str(rerun_failed))
        write(run, "# rfs: " + (rfs_file if rfs else "All"))
        write(run, asheader("REQUESTED RFs QUEUED TO RUN", "# "))
        for rf in queued_rfs:
            write(run, rf)
        write(run, asheader("REQUESTED RFs WITH INVALID STATUS", "## "))
        for rf in rf_status[Status.INVALID]:
            write(run, "## " + rf)
        write(run, asheader("REQUESTED RFs STILL RUNNING", "### "))
        for rf in rf_status[Status.RUNNING]:
            write(run, "### " + rf)
    # Prompt for approval
    if rf_status[Status.INVALID]:
        print("Warning: Found rfs with status INVALID which were ignored")
    if rf_status[Status.RUNNING]:
        print("Warning: Found rfs with status RUNNING which were ignored")
    approval = input("Run file written to " + runfile + " "
         + "("+str(len(queued_rfs)) + " rfs queued to run).\nProceed (y/N)? ")
    if not approval == 'y':
        return print("Aborting run")
    # Start the run
    os.rename(path.join(sim,runfile), path.join(sim,'run',runfile))
    def handle_sigterm(rc, *args):
        print("Sweep terminated by external SIGNAL "+str(rc)+".")
        pool.terminate()
        pool.join()
        raise SystemExit(rc)
    def handle_sigint(*args):
        print("\nSweep interrupted.")
        pool.terminate()
        pool.join()
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)
    def init():
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = multiprocessing.Pool(processes=num_procs, initializer=init)
    args = [(rf, sim, interp, script, script_hash) for rf in queued_rfs]
    pool.imap_unordered(run_rf, args, chunksize=1)
    pool.close()
    print("Sweep started. Press CTRL+C to interrupt.")
    pool.join()

def run_rf(args):
    rf, sim, interp, script, script_hash = args
    rf = path.join(sim, "rfs", rf)
    # Open log and status files
    log = open(path.join(rf,'log.txt'), 'a+')
    status = open(path.join(rf,'status.txt'), 'a+')
    write(log, asheader("LOG FILE OPENED "+get_timestamp()))
    # Check integrity of script
    if '@'.join((script,file_hash(path.join(sim,"bin",script)))) != script_hash:
        raise AssertionError("Incorrect hash for script "+script)
    # Run the process
    try:
        process = subprocess.Popen([interp, path.join(sim,"bin",script), rf],\
            stdout=log, stderr=subprocess.STDOUT)
        # # Define signal and exit handlers
        def sig_handler(rc, *args):
            write(log, "SIGNAL "+str(rc)+" RECEIVED: EXITING")
            write(status, generate_status("  FAILED",script_hash))
            process.kill()
            raise SystemExit(rc)
        signal.signal(signal.SIGQUIT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)
        write(status, generate_status(" STARTED",script_hash))
        rc = process.wait()
        if rc == 0:
            write(status, generate_status("FINISHED",script_hash))
        else:
            write(log, "SCRIPT TERMINATED WITH EXIT CODE "+str(rc))
            write(status, generate_status("  FAILED",script_hash))
    except:
        raise
    finally:
        write(log, asheader("LOG FILE CLOSED "+get_timestamp()))
        log.close()
        status.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('rf', type=str, help="Run folder name")
    parser.add_argument('script', type=str, help="Script to run")
    parser.add_argument('--interpreter', type=str, default='python',\
        help="Interpreter for the script")
    args = parser.parse_args()
    run_sweep(args.interpreter, args.sim, args.script, args.num_procs,\
        args.rerun_failed, args.rfs)
