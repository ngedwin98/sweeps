import argparse
import os, os.path as path, shutil
import signal
import subprocess, multiprocessing

from .sweep_utils import write, get_script_id, get_timestamp, asheader
from .sweep_utils import Status, collect_rf_status, generate_status
from .setup_sweep import read_sweep

def run_sweep(sim, prog, script, num_proc, from_sweep=None, rerun_failed=False,\
    **kwargs):
    timestamp = get_timestamp()
    # Determine status of all requested rfs
    rf_status = collect_rf_status(script, sim)
    if from_sweep is not None:
        rfs = [rf for rf,_ in read_sweep(path.join(sim,from_sweep))]
        for status in Status:
            rf_status[status].intersection_update(rfs)
    queued_rfs = set(rf_status[Status.NEW])
    if rerun_failed:
        queued_rfs.update(rf_status[Status.FAILED])
    # Write summary of rfs status to file
    runfile = timestamp+'.run'
    with open(path.join(sim,runfile), 'a+') as run:
        write(run, "# RUN FILE FOR SWEEP GENERATED AT " + timestamp)
        write(run, "# script: " + get_script_id(script,sim))
        write(run, "# rerun_failed: " + str(rerun_failed))
        write(run, "# rfs: " + (from_sweep if from_sweep else "All"))
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
        return print("Aborting sweep.")
    # Define signal handlers
    def handle_signal(rc, *args):
        if rc == signal.SIGINT:
            print("\nSweep interrupted", end="")
        else:
            print("Sweep received external SIGNAL "+str(rc), end="")
        print(": Terminating processes.")
        pool.terminate()
        pool.join()
        raise SystemExit(rc)
    signal.signal(signal.SIGQUIT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    # Start the sweep
    os.rename(path.join(sim,runfile), path.join(sim,'run',runfile))
    if from_sweep is not None:
        os.rename(path.join(sim,from_sweep), path.join(sim,'run',from_sweep))
    shutil.copyfile(path.join(sim,'bin',script),\
        path.join(sim,'run',timestamp+'.script'))
    pool = multiprocessing.Pool(processes=num_proc)
    args = [(rf, sim, prog, script) for rf in queued_rfs]
    pool.imap_unordered(run_rf, args, chunksize=1)
    pool.close()
    print("Sweep started. Press CTRL+C to interrupt.")
    pool.join()
    print("Sweep completed.")

def run_rf(args):
    rf, sim, interp, script = args
    rf_path = path.join(sim, 'rfs', rf)
    script_path = path.join(sim, 'bin', script)
    script_id = get_script_id(script, sim)
    # Open log and status files
    log = open(path.join(rf_path,'log.txt'), 'a+')
    status = open(path.join(rf_path,'status.txt'), 'a+')
    write(log, asheader("LOG FILE OPENED "+get_timestamp()))
    # Define signal handlers
    def handle_signal(rc, *args):
        write(log, "SIGNAL "+str(rc)+" RECEIVED: TERMINATING SCRIPT")
        process.terminate()
        raise SystemExit(rc)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, handle_signal)
    # Run the script
    process = subprocess.Popen([interp, script_path, rf_path],\
        stdout=log, stderr=subprocess.STDOUT)
    write(status, generate_status(" STARTED",script_id))
    try:
        rc = process.wait()
    except SystemExit as e:
        rc = e.code
        raise e
    finally:
        if rc == 0:
            write(status, generate_status("FINISHED",script_id))
        else:
            write(log, "SCRIPT RETURNED WITH EXIT CODE "+str(rc))
            write(status, generate_status("  FAILED",script_id))
        write(log, asheader("LOG FILE CLOSED "+get_timestamp()))
        log.close()
        status.close()
