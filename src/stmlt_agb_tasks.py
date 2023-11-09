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
import git
import utils
from bokeh.io import export_png

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor



profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write(
    """
Welcome to the abstract graph builder!
"""
)



def graph_components_generator(number_of_tasks):
    """! This function will generate the graph of the entire project

    Args:
        number_of_tasks (int): A number describing the number of tasks to be added

    Returns:
        inputs: A list of input files
        commands: A list of commands (ideally one per task)
        outputs: A list of output files
    """
    nodes = []
    edges = []
    for i in range(number_of_tasks):
        container = st.container()
        with container:
            col1, col2, col4, col5, col6, col7, col8, col9 = st.columns(
                [1, 2, 2, 2, 2, 2, 2, 2]
            )
            stage_type = col1.selectbox("Select node type", ["task"], key=f"stage_{i}")

            task = col2.text_input(
                f"Task {i}", key=f"name_{i}", placeholder="Task Name"
            )

            if not task:  # if there is no task stop the execution
                st.stop()
            inputs_grp = col4.text_input(
                f"Inputs for task {i}", key=f"inps_{i}", placeholder="Inputs"
            )

            outputs_grp = col5.text_input(
                f"Outputs for task {i}", key=f"outps_{i}", placeholder="Outputs"
            )

            command = col6.text_input(
                f"Command for task {i}", key=f"cmd_{i}", placeholder="Command"
            )

            message = col7.text_input(
                f"Message for task {i}", key=f"msg_{i}", placeholder="Message"
            )

            pce = col8.number_input(
                f"PCE for task {i}",
                key=f"pce_{i}",
                value=0,
                min_value=0,
                step=1,
                placeholder="Enter a number",
            )

            subworkflow = col9.text_input(
                f"Subworkflow for task {i}", key=f"wrkf_{i}", placeholder="Subworkflow"
            )

            inputs = []
            outputs = []
            for item in inputs_grp.split(","):
                inps_expanded = utils.file_name_expansion(item)

                if (
                    len(item.rstrip()) == 0
                ):  # if there is no file (or there is an empty file) stop the execution
                    st.stop()

                inputs.extend(inps_expanded)

            for item in outputs_grp.split(","):
                outps_expanded = utils.file_name_expansion(item)

                if (
                    len(item.rstrip()) == 0
                ):  # if there is no file (or there is an empty file) stop the execution
                    st.stop()

                outputs.extend(outps_expanded)

            if not subworkflow:
                subworkflow = "main"
            nodes.append(
                (
                    task,
                    {
                        "description": task,
                        "command": command,
                        "inputs": inputs,
                        "outputs": outputs,
                        "message": message,
                        "PCE": pce,
                        "subworkflow": subworkflow,
                    },
                )
            )

    for node1 in nodes:
        for node2 in nodes:
            diff_set = set(node1[1]["outputs"]).intersection(set(node2[1]["inputs"]))
            if diff_set:
                edges.append((node1[0], node2[0]))

    return nodes, edges


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

    workflows = nx.get_node_attributes(gdb.graph, "subworkflow").values()
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
            if v["subworkflow"] == flow
        ]

        body_list = []
        for task in task_nodes:
            # inputs  = gdb.graph.predecessors(task)
            # outputs = gdb.graph.successors(task)
            inputs = gdb.graph.nodes[task]["inputs"][0].split(",")
            outputs = gdb.graph.nodes[task]["outputs"][0].split(",")
            command = gdb.graph.nodes[task]["command"]

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
                    targets=[ast.Name(id="command", ctx=ast.Store())],
                    value=ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id=task, ctx=ast.Load()),
                            attr="command",
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


def export_graph_tasks(**kwargs):
    """! This function will export the graph to Pedro's notation and
    throws an exception to streamlit if there is some error
    """
    try:
        nodes = kwargs["graph"].graph.nodes(data=True)
        path = Path(kwargs["filename"])
        with open(path, "w") as file_abs:
            for node in nodes:
                file_abs.writelines(
                    f"T<>{node[0]}<>{','.join(node[1]['inputs'])}<>{','.join(node[1]['outputs'])}<>{node[1]['command']}<>{node[1]['PCE']}<>{node[1]['subworkflow']}<>{node[1]['message']}\n"
                )

        # kwargs["graph"].graph_export(kwargs["filename"])
    except Exception as exception_graph:
        st.sidebar.text(f"{exception_graph}")






def _get_commit_list(self, commits):
    """! This function will append to run_cmd_commits if there is a DATALAD RUNCMD"""
    return [item for item in commits if "DATALAD RUNCMD" in item.message]


def _commit_message_node_extract(self, commit):
    return ast.literal_eval(
        re.search("(?=\{)(.|\n)*?(?<=\}\n)", commit.message).group(0)
    )


def _get_dataset(self, dataset):
    """! This function will return a Datalad dataset for the given path
    Args:
        dataset (str): _description_
    Returns:
        dset (Dataset): A Datalad dataset
    """
    dset = dl.Dataset(dataset)
    if dset is not None:
        return dset
    

def _get_superdataset(self, dataset):
    """! This function will return the superdataset
    Returns:
        sds/dset (Dataset): A datalad superdataset
    """
    dset = dl.Dataset(dataset)
    sds = dset.get_superdataset()
    if sds is not None:  # pylint: disable = no-else-return
        return sds
    else:
        return dset
    

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
            print('dictionary', dict_o)
            task = graphs.TaskWorkflow(
                self.superdataset.path,
                dict_o["cmd"],
                commit.hexsha,
                commit.author.name,
                commit.authored_date,
            )
            if dict_o["inputs"]:
                for input_file in dict_o["inputs"]:
                    input_path = glob.glob(
                        self.superdataset.path
                        + f"/**/*{os.path.basename(input_file)}",
                        recursive=True,
                    )[0]
                    task.inputs.append(input_path)
            
            if dict_o["outputs"]:
                for output_file in dict_o["outputs"]:
                    output_path = glob.glob(
                        self.superdataset.path 
                        + f"/**/*{os.path.basename(output_file)}",
                        recursive=True,
                    )[0]
                    task.outputs.append(output_path)
            node_list.append((task.commit, task.__dict__))
        for idx_node, node1 in enumerate(node_list):
            for node2 in node_list[:idx_node + 1]:
                diff_set = set(node1[1]["outputs"]).intersection(set(node2[1]["inputs"]))
                if diff_set:
                    edge_list.append((node1[0], node2[0]))
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
        gdb_abstract = utils.graph_relabel(gdb_abstract, node_mapping)

        # except Exception as err:
        #     st.warning(
        #         f"Error creating graph object. Please check that your dataset path contains a valid Datalad dataset"
        #     )
        #     st.stop()

        gdb_abstract, gdb_difference = utils.graph_diff(gdb_abstract, gdb_provenance)

        graph_plot_abs = gdb_abstract.graph_object_plot()
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
            for predecessors in gdb_difference.graph.predecessors(item):
                inputs_dict[predecessors] = gdb_difference.graph.nodes[predecessors]
                inputs.append(gdb_difference.graph.nodes[predecessors])

            for successors in gdb_difference.graph.successors(item):
                outputs_dict[successors] = gdb_difference.graph.nodes[successors]

            inputs = list(inputs_dict.keys())
            outputs = list(outputs_dict.keys())
            dataset = utils.get_git_root(os.path.dirname(inputs[0]))
            command = gdb_difference.graph.nodes[item]["cmd"]
            message = "test"

            scheduler.add_job(
                utils.job_submit,
                args=[dataset, branch, inputs, outputs, message, command],
            )

    except Exception as err:  # pylint: disable = bare-except
        st.warning(
            f"No provance graph has been matched to this abstract graph, match one first {err}"
        )


def graph_object_plot(graph_input, fc="node_color"):
    """! Utility to generate a plot for a networkx graph
    Args:
        graph_nx (graph): A networkx graph
    Returns:
        plot: A graphviz figure to be plotted with bokeh
    """
    # The next two lines are to fix an issue with bokeh 3.3.0 if using bokeh 2.4.3 these can be removed
    mapping = dict((n, i) for i, n in enumerate(graph_input.nodes))
    H = nx.relabel_nodes(graph_input, mapping=mapping)
    nx.set_node_attributes(H, "grey", name=fc)   # adding grey color at initialization
    graph_layout = graphviz_layout(
        H, prog="dot", root=None, args="-Gnodesep=1000 -Grankdir=TB"
    )
    graph = from_networkx(H, graph_layout)
    plot = figure(
        title="File provenance tracker",
        toolbar_location="below",
        tools="pan,wheel_zoom",
    )
    plot.axis.visible = False
    plot.x_range = DataRange1d(range_padding=0.5)
    plot.y_range = DataRange1d(range_padding=0.5)
    node_hover_tool = HoverTool(
        tooltips=[
            ("index", "@index"),
            ("description", "@description"),
            ("subworkflow", "@subworkflow"),
            ("inputs", "@inputs"),
            ("outputs", "@outputs"),
            ("command", "@command"),
            ("message", "@message"),
            ("PCE", "@PCE")
        ]
    )
    plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())
    graph.node_renderer.glyph = Circle(size=20, fill_color=fc)
    plot.renderers.append(graph)
    x_coord, y_coord = zip(*graph.layout_provider.graph_layout.values())
    node_labels = nx.get_node_attributes(graph_input, "description")
    node_names = list(node_labels.values())
    source = ColumnDataSource({"x": x_coord, "y": y_coord, "description": node_names})
    labels = LabelSet(
        x="x",
        y="y",
        text="description",
        source=source,
        background_fill_color="white",
        text_align="center",
        y_offset=11,
    )
    plot.renderers.append(labels)
    return plot

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
        node_list, edge_list = utils.gcg_processing_tasks(args.agraph)

    else:
        tasks_number = st.number_input("Please define a number of stages", min_value=1)
        node_list, edge_list = graph_components_generator(tasks_number)

    try:
        gdb = nx.DiGraph()
        gdb.add_nodes_from(node_list)
        gdb.add_edges_from(edge_list)
        st.success("Graph created")

    except ValueError as e:
        st.warning(
            f"There was a problem in the creation of the graph verify\
                   that all node names match along the edges {e}"
        )
        st.stop()

    graph_plot_abstract = graph_object_plot(gdb)
    plot_graph(graph_plot_abstract)
    if args.png_export:
        export_png(graph_plot_abstract, filename=args.png_export)

    export_name = st.sidebar.text_input("Path for abstract graph export")

    st.sidebar.button(
        "Save",
        on_click=export_graph_tasks,
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
