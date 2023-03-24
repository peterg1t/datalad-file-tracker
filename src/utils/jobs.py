import os
import datalad.api as dl
import utils
import asyncio
import git
import subprocess

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor


def command_submit(command):
    command_run_output = subprocess.run(
        command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = command_run_output.stdout.split("\n")
    errlog = command_run_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element

    return outlog, errlog


def job_prepare(dataset,branch):
    outlogs = []
    errlogs = []

    print(dataset)
    worktree_command = f"cd {dataset}; git worktree add .wt/{branch}_wt {branch}"
    outlog, errlog = command_submit(worktree_command)

    outlogs.append(('dataset->', dataset))
    outlogs.append(outlog)
    errlogs.append(errlog)

    print('logs',outlogs, errlogs)





def job_clean(dataset):
    outlogs = []
    errlogs = []

    worktree_rm_command = f"cd {dataset}; rm -rf .wt/"
    outlog, errlog = command_submit(worktree_rm_command)
    outlogs.append(outlog)
    errlogs.append(errlog)

    worktree_prune_command = f"cd {dataset}; git worktree prune"
    outlog, errlog = command_submit(worktree_prune_command)
    outlogs.append(outlog)
    errlogs.append(errlog)

    print('logs',outlogs, errlogs)



def run_pending_nodes(gdb_abstract, gdb_difference, branch):
    """! Given a graph and the list of nodes (and requirements i.e. inputs)
    compute the task with APScheduler

    Args:
        gdb_difference (graph): An abstract graph
    """
    inputs =[] 
    outputs=[]
    # try:
    next_nodes_req = gdb_difference.next_nodes_run()
    print('next_nodes_req', next_nodes_req)
   
    for item in next_nodes_req:
        inputs.extend([p for p in gdb_abstract.graph.predecessors(item)])
        outputs.extend([s for s in gdb_abstract.graph.successors(item)])
    if inputs:
        if (not all( [os.path.isabs(f) for f in outputs] ) or not all( [os.path.isabs(f) for f in inputs] )) == False:
            try:
                dataset = utils.get_git_root(os.path.dirname(inputs[0]))
                superdataset = utils.get_superdataset(dataset)
            except Exception as e:
                print(f"There are no inputs -> {e}")

            command = gdb_difference.graph.nodes[item]["cmd"]
            message = "test"

            job_submit(superdataset.path, branch, inputs, outputs, message, command)
            
        # scheduler.add_job(job_submit, args=[superdataset, inputs, outputs, message, command])


    # except Exception as e:  # pylint: disable = bare-except
    #     print(
    #         f"No provance graph has been matched to this abstract graph, match one first {e}" 
    #     )





    



def job_submit(superdataset, branch, inputs, outputs, message, command):
    """! This function will execute the datalad run command

    Args:
        dataset (str): Path to the dataset
        input (str): Path to input
        output (str): Path to output
        message (str): Commit message
        command (str): Commmand

    Raises:
        Exception: If error is found
    """
    outlogs = []
    errlogs = []

    #for worktrees
    # ds = utils.get_git_root(os.path.dirname(outputs[0]))
    # rel_path_outputs = os.path.relpath(ds, superdataset)
    # outputs_worktree = [f"{superdataset}/.wt/{branch}_wt/{os.path.join(rel_path_outputs, os.path.basename(o))}" for o in outputs]

    # making the output stage folder
    if os.path.exists(os.path.dirname(outputs[0])):
        pass
    else:
        os.mkdir(os.path.dirname(outputs[0]))

    inputs_proc = " -i ".join(inputs)
    outputs_proc = " -o ".join(outputs)
    # saving the dataset prior to processing
    dl.save(path=superdataset, dataset=superdataset)

    containers_run_command = f"cd {superdataset}; datalad run -m '{message}' -d '{superdataset}' -i {inputs_proc} -o {outputs_proc} '{command}'"
    # containers_run_command = f"cd {superdataset}/.wt/{branch}_wt; datalad run -m '{message}' -d '{superdataset}' -i {inputs_proc} -o {outputs_proc} '{command}'"
    
    outlog, errlog = command_submit(containers_run_command)
    outlogs.append(outlog)
    errlogs.append(errlog)
    for item in errlogs[0]:
        if "error" in item:
            raise Exception(
                "Error found in the datalad containers run command, check the log for more information on this error."
            )
    
    print('logs',outlogs, errlogs)
