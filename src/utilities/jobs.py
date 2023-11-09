import os
import datalad.api as dl
import utilities
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



def run_pending_nodes(original_ds, dataset, gdb_abstract, gdb_difference, branch):
    """_summary_

    Args:
        original_ds (str): The original dataset
        dataset (str): A path to the dataset
        gdb_abstract (graph): An abstract graph
        gdb_difference (graph): A graph for the difference
        branch (str): The branch that is running

    Returns:
        _type_: _description_
    """
    inputs =[] 
    outputs=[]
    input_datasets =[]
    output_datasets =[]
    # try:
    next_nodes_req = gdb_difference.next_nodes_run()
    print('Inputs')
    print(original_ds)
    print(dataset)
    print(branch)

    print('next_nodes_req', next_nodes_req, 'branch->', branch)
   
    for item in next_nodes_req:
        inputs.extend([os.path.join(dataset,os.path.relpath(p, original_ds)) for p in gdb_abstract.graph.predecessors(item)])
        outputs.extend([os.path.join(dataset,os.path.relpath(s, original_ds)) for s in gdb_abstract.graph.successors(item)])
        
    
    if inputs:
        if ( (all( [os.path.exists(os.path.dirname(f)) for f in outputs] ) and all( [os.path.exists(os.path.dirname(f)) for f in inputs] ))):
            command = gdb_difference.graph.nodes[item]["cmd"]
            message = "test"

            return job_submit(dataset, branch, inputs, outputs, message, command)

            
 




    



def job_submit(dataset, branch, inputs, outputs, message, command):
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
    print('submitting job', inputs, outputs, branch, message, command)

    # making the output stage folder
    if os.path.exists(os.path.dirname(outputs[0])):
        pass
    else:
        os.mkdir(os.path.dirname(outputs[0]))

    inputs_proc = " -i ".join(inputs)
    outputs_proc = " -o ".join(outputs)
    # saving the dataset prior to processing
    dl.save(path=dataset, dataset=dataset)

    datalad_run_command = f"cd {dataset}; datalad run -m '{message}' -d '{dataset}' -i {inputs_proc} -o {outputs_proc} '{command}'"
    print('command->', datalad_run_command, 'branch->', branch)
    
    outlog, errlog = command_submit(datalad_run_command)
    outlogs.append(outlog)
    errlogs.append(errlog)
    for item in errlogs[0]:
        if "error" in item:
            raise Exception(
                "Error found in the datalad containers run command, check the log for more information on this error."
            )
    
    print('logs',outlogs, errlogs)
    return outlogs
