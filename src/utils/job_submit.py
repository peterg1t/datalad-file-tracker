import os
import datalad.api as dl
import utils
import subprocess


def command_submit(command):
    command_run_output = subprocess.run(
        command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = command_run_output.stdout.split("\n")
    errlog = command_run_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element

    return outlog, errlog


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


    inputs_proc = ' -i '.join(inputs)
    outputs_proc = ' -o '.join(outputs)


    # saving the dataset prior to processing
    dl.save(path=dataset, dataset=dataset)

    containers_run_command = f"cd {dataset} && datalad run -m '{message}' -d '{dataset}' -i {inputs_proc} -o {outputs_proc} '{command}'"
    print("full run command", containers_run_command)
    # containers_run_command = f"which datalad; datalad wtf -S extensions"
    outlog, errlog = command_submit(containers_run_command)
    outlogs.append(outlog)
    errlogs.append(errlog)
    print("logs_job", outlogs, errlogs)
    for item in errlogs[0]:
        if "error" in item:
            raise Exception(
                "Error found in the datalad containers run command, check the log for more information on this error."
            )
