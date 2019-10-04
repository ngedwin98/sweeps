import sweeps
import argparse
import multiprocessing

def main():
    # Set up 
    parser = argparse.ArgumentParser(prog="sweeps",\
        description="Run parameter sweeps")
    parser.add_argument('sim', help="Sweep directory")
    subparsers = parser.add_subparsers(required=True)
    setup = subparsers.add_parser('setup', help="Setup sweep")
    run = subparsers.add_parser('run', help="Run sweep")
    # setup command
    setup.add_argument('sweep_file', "Sweep .json file")
    subparsers = setup.add_subparsers(required=True)
    create = subparsers.add_parser('create', help="Create rfs")
    create.set_defaults(func=sweeps.create_rfs)
    delete = subparsers.add_parser('delete', help="Delete rfs")
    delete.set_defaults(func=sweeps.delete_rfs)
    # run command
    run.add_argument('program', help="Interpreter for script")
    run.add_argument('script', help="Run script location relative to sim/bin")
    run.add_argument('num_proc', type=int, default=multiprocessing.cpu_count(),\
        help="Number of processes")
    run.add_argument('--from_sweep', help="Restrict rfs to this sweep file")
    run.add_argument('--rerun-failed', action='store_true',\
        help="Rerun failed rfs")
    run.set_defaults(func=sweeps.run_sweep)
    # Execute
    args = parser.parse_args()
    args.func(**args)

if __name__ == "__main__":
    main()
