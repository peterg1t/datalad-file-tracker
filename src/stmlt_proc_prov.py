"""
Docstring
"""
import os
import argparse
import git
from itertools import repeat
from multiprocessing import Pool

import cProfile

import graphs
from bokeh.transform import linear_cmap

import utils


profiler = cProfile.Profile()


def get_data(run):
    pass



def match_run(abstract, provenance, runs):
    print(abstract, provenance, runs)
    node_abstract_list, edge_abstract_list = utils.gcg_processing(abstract)
    
    gdb_abs = graphs.GraphBase(node_abstract_list, edge_abstract_list)
    # for one run
    # gdb_prov = graphs.GraphProvenance(provenance, run)
    # for several runs we are going to create a pool
    for run in runs:
        #checkout run and clone to /tmp
        repo = git.Repo(provenance)
        
        repo.heads[run].clone(os.path.join("/tmp", run))
        # cloned_repo = repo.clone(os.path.join("/tmp", run))
        # assert cloned_repo.__class__ is git.Repo  # clone an existing repository
        # assert git.Repo.init(os.path.join("/tmp", run)).__class__ is git.Repo
        
    #perform datalad get on all cloned repos
    # with Pool(4) as p:
    #     p.starmap(get_data, zip(repeat(gdb_abs), runs))




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a",
        "--agraph",
        type=str,
        help="Path to graph txt file. \
                        Content must have the F<>{files}<>{prec_nodes} format per line\
                        or  T<>{task}<>{command}<>{prec_nodes}<>{transformation}   ",
        required=True,
    )

    parser.add_argument(
        "-p",
        "--pgraph",
        type=str,
        help="Path to provenance dataset (superdataset)",
        required=True,
    )

    parser.add_argument(
        "-r", "--runs", nargs="+", help="Run number to match", required=True
    )

    args = parser.parse_args()  # pylint: disable = invalid-name

    abspath = args.agraph
    provpath = args.pgraph
    runs = args.runs

    # Match run
    match_run(abspath, provpath, runs)
