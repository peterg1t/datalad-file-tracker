"""
Docstring
"""
import os
import sys
import argparse
import copy
import ast
from pathlib import Path
import cProfile
import streamlit as st
import csv
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
from bokeh.plotting import from_networkx, figure
from bokeh.models import (
    BoxZoomTool,
    Circle,
    HoverTool,
    ResetTool,
    ColumnDataSource,
    LabelSet,
    DataRange1d,
)
from bokeh.io import export_png
import datalad.api as dl
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

import utils


profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write(
    """
Welcome to the abstract graph builder!
"""
)



def plot_graph(plot):
    """! Function to generate a bokeh chart

    Args:
        plot (plot): A networkx plot
    """
    st.bokeh_chart(plot, use_container_width=True)


def generate_code(gdb):
    module = ast.Module(
        body=[
            ast.Import(names=[ast.alias(name="asyncio")]),
            ast.ImportFrom(module="prefect", names=[ast.alias(name="flow")], level=0),
            ast.ImportFrom(
                module="prefect.task_runners",
                names=[
                    ast.alias(name="SequentialTaskRunner"),
                    ast.alias(name="ConcurrentTaskRunner"),
                ],
                level=0,
            ),
            ast.ImportFrom(
                module="prefect_dask.task_runners",
                names=[ast.alias(name="DaskTaskRunner")],
                level=0,
            ),
        ],
        type_ignores=[],
    )

    workflows = nx.get_node_attributes(gdb, "workflow").values()
    workflows_unique = list(dict.fromkeys(workflows))

    flow_list = []
    for flow in workflows_unique:
        flow_list.append(
            ast.Expr(
                value=ast.Call(
                    func=ast.Name(id=flow, ctx=ast.Load()), args=[], keywords=[]
                )
            )
        )

        task_nodes = [
            n
            for n, v in gdb.graph.nodes(data=True)
            if v["type"] == "task" and v["workflow"] == flow
        ]

        body_list = []
        for task in task_nodes:
            inputs = gdb.predecessors(task)
            outputs = gdb.successors(task)
            command = gdb.nodes[task]["cmd"]

            body_list.append(
                ast.Assign(
                    targets=[ast.Name(id=task, ctx=ast.Store())],
                    value=ast.Call(
                        func=ast.Name(id="task_build", ctx=ast.Load()),
                        args=[],
                        keywords=[
                            ast.keyword(
                                arg="inputs",
                                value=ast.List(
                                    elts=[
                                        ast.Name(id=inp, ctx=ast.Load())
                                        for inp in inputs
                                    ]
                                ),
                            ),
                            ast.keyword(
                                arg="outputs",
                                value=ast.List(
                                    elts=[
                                        ast.Name(id=out, ctx=ast.Load())
                                        for out in outputs
                                    ]
                                ),
                            ),
                            ast.keyword(
                                arg="task_name", value=ast.Constant(value=command)
                            ),
                            ast.keyword(
                                arg="tmp_dir",
                                value=ast.Name(id="tmp_dir", ctx=ast.Load()),
                            ),
                        ],
                    ),
                )
            )
            body_list.append(
                ast.Assign(
                    targets=[ast.Name(id="cmd", ctx=ast.Store())],
                    value=ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id=task, ctx=ast.Load()),
                            attr="cmd",
                            ctx=ast.Load(),
                        ),
                        args=[],
                        keywords=[],
                    ),
                )
            )

        module.body.append(
            ast.FunctionDef(
                name=flow,
                args=ast.arguments(
                    posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]
                ),
                body=body_list,
                decorator_list=[
                    ast.Call(
                        func=ast.Name(id="flow", ctx=ast.Load()),
                        args=[],
                        keywords=[
                            ast.keyword(
                                arg="task_runner",
                                value=ast.Call(
                                    func=ast.Name(
                                        id="Enter Runner Type Here", ctx=ast.Load()
                                    ),
                                    args=[],
                                    keywords=[],
                                ),
                            )
                        ],
                    )
                ],
            )
        )

    module.body.append(
        ast.If(
            test=ast.Compare(
                left=ast.Name(id="__name__", ctx=ast.Load()),
                ops=[ast.Eq()],
                comparators=[ast.Constant(value="__main__")],
            ),
            body=[flow_list],
            orelse=[],
        )
    )

    module = ast.fix_missing_locations(module)
    code = ast.unparse(module)
    return code


def export_graph(**kwargs):
    """! This function will export the graph to Pedro's notation and
    throws an exception to streamlit if there is some error
    """
    try:
        nodes = kwargs["graph"].nodes(data=True)
        with open(kwargs["filename"], "w") as file_abs:
            for node in nodes:
                if "cmd" in node[1]:
                    file_abs.writelines(
                        f"{node[1]['type'][0].upper()}<>{node[0]}<>{','.join(node[1]['predecesor'])}<>{node[1]['cmd']}<>{node[1]['workflow']}\n"
                    )
                else:
                    file_abs.writelines(
                        f"{node[1]['type'][0].upper()}<>{node[0]}<>{','.join(node[1]['predecesor'])}\n"
                    )
        # kwargs["graph"].graph_export(kwargs["filename"])
    except Exception as exception_graph:
        st.sidebar.text(f"{exception_graph}")


    
def prov_scan(self):
    """! This function will return the nodes and edges list
    Args:
        ds_name (str): A path to the dataset (or subdataset)
    Returns:
        graph: A networkx graph
    """
    node_list = []
    edge_list = []
    # subdatasets = self.superdataset.subdatasets()
    subdatasets = [self.dataset.path]
    for subdataset in subdatasets:
        repo = git.Repo(subdataset)
        commits = list(repo.iter_commits(repo.heads[self.ds_branch]))
        dl_run_commits = _get_commit_list(commits)
        for commit in dl_run_commits:
            dict_o = _commit_message_node_extract(commit)
            task = graphs.TaskWorkflow(
                self.superdataset.path,
                dict_o["cmd"],
                commit.hexsha,
                commit.author.name,
                commit.authored_date,
            )
            if dict_o["inputs"]:
                task.parent_files = dict_o["inputs"]
                for input_file in task.parent_files:
                    input_path = glob.glob(
                        self.superdataset.path
                        + f"/**/*{os.path.basename(input_file)}",
                        recursive=True,
                    )[0]
                    ds_file = git.Repo(os.path.dirname(input_path))
                    file_status = dl.status(
                        path=input_path, dataset=ds_file.working_tree_dir
                    )[0]
                    file = graphs.FileWorkflow(
                        subdataset,
                        input_path,
                        commit.hexsha,
                        commit.author.name,
                        commit.authored_date,
                        file_status["gitshasum"],
                    )
                    file.ID = utils.base_conversions(file.name)
                    file.child_task = dict_o["cmd"]
                    # Creating a shallow copy of the object attribute dictionary
                    dict_file = copy.copy(file.__dict__)
                    dict_file.pop("child_task", None)
                    node_list.append((file.name, dict_file))
                    edge_list.append((file.name, task.commit))
            if dict_o["outputs"]:
                task.child_files = dict_o["outputs"]
                for output_file in task.child_files:
                    output_path = glob.glob(
                        self.superdataset.path + f"/**/*{os.path.basename(output_file)}",
                        recursive=True,
                    )[0]
                    ds_file = git.Repo(os.path.dirname(output_path))
                    file_status = dl.status(
                        path=output_path, dataset=ds_file.working_tree_dir
                    )[0]
                    file = graphs.FileWorkflow(
                        subdataset,
                        output_path,
                        commit.hexsha,
                        commit.author.name,
                        commit.authored_date,
                        file_status["gitshasum"],
                    )
                    file.ID = utils.base_conversions(file.name)
                    file.parent_task = dict_o["cmd"]
                    dict_file = copy.copy(file.__dict__)
                    dict_file.pop("parent_task", None)
                    node_list.append((file.name, dict_file))
                    edge_list.append((task.commit, file.name))
            node_list.append((task.commit, task.__dict__))
    return node_list, edge_list



def match_graphs(provenance_ds_path, gdb_abstract, ds_branch):
    """! Function to match the graphs loaded with Streamlit interface

    Args:
        provenance_ds_path (str)`: The path to the provenance dataset
        gdb_abstract (graph): An abstract graph
    """
    node_mapping = {}
    repo = git.Repo(provenance_ds_path)
    branch = repo.heads[ds_branch]
    branch.checkout()
    with open(f"{provenance_graph_path}/tf.csv", "r") as translation_file:
        reader = csv.reader(translation_file)
        for row in reader:
            node_mapping[row[0]] = f"{provenance_graph_path}/{row[1]}"

    if utils.exists_case_sensitive(provenance_ds_path):
        # try:
        gdb_provenance = graphs.GraphProvenance(provenance_ds_path, ds_branch)
        print("b4", gdb_abstract.graph.nodes)
        gdb_abstract = utils.graph_relabel(gdb_abstract, node_mapping)
        print("aft", gdb_abstract.graph.nodes(data=True))

        # except Exception as err:
        #     st.warning(
        #         f"Error creating graph object. Please check that your dataset path contains a valid Datalad dataset"
        #     )
        #     st.stop()

        gdb_abstract, gdb_difference = utils.graph_diff(gdb_abstract, gdb_provenance)

        graph_plot_abs = graph_object_plot(gdb_abstract)
        plot_graph(graph_plot_abs)

        # graph_plot_diff = gdb_difference.graph_object_plot()
        # plot_graph(graph_plot_diff)

        next_nodes_requirements = gdb_difference.next_nodes_run()

        if "next_nodes_req" not in st.session_state:
            st.session_state["next_nodes_req"] = next_nodes_requirements

    else:
        st.warning(f"Path {provenance_ds_path} does not exist.")
        st.stop()

    return gdb_difference





def run_pending_nodes(gdb_difference, branch):
    """! Given a graph and the list of nodes (and requirements i.e. inputs)
    compute the task with APScheduler

    Args:
        gdb_difference (graph): An abstract graph
    """
    inputs_dict = {}
    outputs_dict = {}
    inputs = []

    # we need to use the translation file so the nodes in the difference tree have the file names instead of the abstract names. From the nodes we can extract the list of inputs and outputs for the job that is going to run
    node_mapping = {}
    with open(f"{provenance_graph_path}/tf.csv", "r") as translation_file:
        reader = csv.reader(translation_file)
        for row in reader:
            node_mapping[row[0]] = f"{provenance_graph_path}/{row[1]}"

    gdb_difference = utils.graph_relabel(gdb_difference, node_mapping)

    try:
        next_nodes_req = st.session_state["next_nodes_req"]
        for item in next_nodes_req:
            for predecessors in gdb_difference.predecessors(item):
                print("predecessors", predecessors)
                inputs_dict[predecessors] = gdb_difference.nodes[predecessors]
                inputs.append(gdb_difference.nodes[predecessors])

            for successors in gdb_difference.successors(item):
                outputs_dict[successors] = gdb_difference.nodes[successors]

            print("inputs_dict", inputs_dict)
            inputs = list(inputs_dict.keys())
            print("inputs_dict2", inputs)

            outputs = list(outputs_dict.keys())
            dataset = utils.get_git_root(os.path.dirname(inputs[0]))
            command = gdb_difference.nodes[item]["cmd"]
            message = "test"

            print("submit_job", dataset, inputs, outputs, message, "command=", command)
            scheduler.add_job(
                utils.job_submit,
                args=[dataset, branch, inputs, outputs, message, command],
            )

    except Exception as err:  # pylint: disable = bare-except
        st.warning(
            f"No provance graph has been matched to this abstract graph, match one first {err}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Added argument parser to parse a file with a workflow from a text file, the text file
    # format will be the following format
    # {inputs}<>{task}<>{outputs}
    parser.add_argument(
        "-a",
        "--agraph",
        type=str,
        help="Path to graph txt file. \
                        Content must have the F<>{files}<>{prec_nodes} format per line\
                        or  T<>{task}<>{prec_nodes}<>{command}<>{workflow}   ",
    )
    parser.add_argument(
        "-p", "--pgraph", type=str, help="Path to project to extract provenance"
    )
    parser.add_argument(
        "-gml",
        "--gml_export",
        type=str,
        help="Flag to export abstract graph to GML format",
    )
    parser.add_argument(
        "-png",
        "--png_export",
        type=str,
        help="Flag to export abstract graph to png format",
    )

    args = parser.parse_args()  # pylint: disable = invalid-name

    # We now start the background scheduler
    # scheduler = BackgroundScheduler()
    # This will get you a BackgroundScheduler with a MemoryJobStore named
    # “default” and a ThreadPoolExecutor named “default” with a default
    # maximum thread count of 10.

    # Lets cutomize the scheduler a little bit lets keep the default
    # MemoryJobStore but define a ProcessPoolExecutor
    jobstores = {
        "default": SQLAlchemyJobStore(
            url="sqlite:////Users/pemartin/Projects/datalad-file-tracker/src/jobstore.sqlite"
        )
    }
    executors = {
        "default": ThreadPoolExecutor(8),
    }
    job_defaults = {"coalesce": False, "max_instances": 3}
    scheduler = BackgroundScheduler(
        jobstores=jobstores, executors=executors, job_defaults=job_defaults
    )
    scheduler.start()  # We start the scheduler

    next_nodes_req = []

    node_list = None  # pylint: disable=invalid-name
    edge_list = None  # pylint: disable=invalid-name

    if args.agraph:
        node_list, edge_list = utils.gcg_processing(args.agraph)

    else:
        tasks_number = st.number_input("Please define a number of stages", min_value=1)
        node_list, edge_list = utils.graph_components_generator(tasks_number)

    try:
        gdb = nx.DiGraph()
        gdb.add_nodes_from(node_list)
        gdb.add_edges_from(edge_list)
        st.success("Graph created")

    except:
        st.warning(
            f"There was a problem in the creation of the graph verify\
                   that all node names match along the edges"
        )
        st.stop()

    graph_plot_abstract = graph_object_plot(gdb)
    plot_graph(graph_plot_abstract)
    if args.png_export:
        export_png(graph_plot_abstract, filename=args.png_export)

    export_name = st.sidebar.text_input("Path for abstract graph export")

    st.sidebar.button(
        "Save",
        on_click=export_graph,
        kwargs={"graph": gdb, "filename": export_name},
    )

    # The provenance graph name is the path to any
    # directory in a project where provenance is recorded.
    # When the button is clicked a full provenance graph
    # for all the project is generated and matched
    # to the abstract graph
    provenance_graph_path = st.sidebar.text_input("Path to the dataset with provenance")

    if st.sidebar.button("Generate code"):
        code = generate_code(gdb)
        st.text_area("Prefect code", code)

    if utils.exists_case_sensitive(provenance_graph_path):
        branches_project = utils.get_branches(provenance_graph_path)
        branch_select = st.sidebar.selectbox("Branches", branches_project)
        match_button = st.sidebar.button("Match")

        if match_button:
            match_graphs(provenance_graph_path, gdb, branch_select)
        run_next_button = st.sidebar.button("Run pending nodes")
        if run_next_button:
            run_pending_nodes(gdb, branch_select)
