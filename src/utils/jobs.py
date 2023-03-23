import os
import datalad.api as dl
import utils
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



def run_pending_nodes(gdb_abstract, gdb_difference):
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


    if (not all( [os.path.isabs(f) for f in outputs] ) or not all( [os.path.isabs(f) for f in inputs] )) == False:
        dataset = utils.get_git_root(os.path.dirname(inputs[0]))
        superdataset = utils.get_superdataset(dataset)
        command = gdb_difference.graph.nodes[item]["cmd"]
        message = "test"

        # jobstores = {
        # "default": SQLAlchemyJobStore(
            # url="sqlite:////Users/pemartin/Projects/datalad-file-tracker/src/jobstore.sqlite"
        # )
        # }
        # executors = {
            # "default": ThreadPoolExecutor(8),
        # }
        # job_defaults = {"coalesce": False, "max_instances": 3}
        # scheduler = BackgroundScheduler(
            # jobstores=jobstores, executors=executors, job_defaults=job_defaults
        # )
        # scheduler.start()  # We start the scheduler

        print("submit_job", superdataset, dataset, inputs, outputs, message, command)
        job_submit(superdataset.path, inputs, outputs, message, command)
        # scheduler.add_job(job_submit, args=[superdataset, inputs, outputs, message, command])


    # except Exception as e:  # pylint: disable = bare-except
    #     print(
    #         f"No provance graph has been matched to this abstract graph, match one first {e}" 
    #     )




def job_submit(dataset, inputs, outputs, message, command):
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


    # making the output stage folder
    if os.path.exists(os.path.dirname(outputs[0])):
        pass
    else:
        os.mkdir(os.path.dirname(outputs[0]))

    inputs_proc = " -i ".join(inputs)
    outputs_proc = " -o ".join(outputs)

    # saving the dataset prior to processing
    dl.save(path=dataset, dataset=dataset)

    containers_run_command = f"cd {dataset} && datalad run -m '{message}' -d '{dataset}' -i {inputs_proc} -o {outputs_proc} '{command}'"
    # containers_run_command = f"which datalad; datalad wtf -S extensions"
    outlog, errlog = command_submit(containers_run_command)
    outlogs.append(outlog)
    errlogs.append(errlog)
    for item in errlogs[0]:
        if "error" in item:
            raise Exception(
                "Error found in the datalad containers run command, check the log for more information on this error."
            )
    
    print('logs',outlogs, errlogs)
