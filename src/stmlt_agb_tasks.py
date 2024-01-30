"""
Docstring
"""
import argparse
import ast
import cProfile
import csv
import os
from pathlib import Path

import git
import networkx as nx
import streamlit as st
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from bokeh.io import export_png
from bokeh.models import (  # type: ignore
    BoxZoomTool,
    Circle,
    ColumnDataSource,
    DataRange1d,
    HoverTool,
    LabelSet,
    ResetTool,
)
from bokeh.plotting import figure, from_networkx
from networkx.drawing.nx_agraph import graphviz_layout

import graphs  # pylint: disable=import-error
import match  # pylint: disable=import-error
import utilities  # pylint: disable=import-error

profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write(
    """
Welcome to the abstract graph builder!
"""
)


def scheduler_configuration() -> int:
    """
    Initializes and customizes a background scheduler for task management.

    This function creates a BackgroundScheduler instance with a customized configuration,
    including a SQLAlchemyJobStore as the jobstore, a ThreadPoolExecutor with 8 threads
    as the executor, and specific job defaults.

    Returns:
    str: The current state of the scheduler.

    Notes:
    - The default jobstore is a MemoryJobStore, but in this customization, it is replaced
      with an SQLAlchemyJobStore using an SQLite database at the specified URL.
    - The default executor is a ThreadPoolExecutor, and its thread count is set to 8.
    - Job defaults include "coalesce" set to False and "max_instances" set to 3.
    - The scheduler is started after customization.

    Example:
    ```python
    scheduler_state = initialize_custom_scheduler()
    print(f"The scheduler is initialized with state: {scheduler_state}")
    ```
    """
    # We now start the background scheduler
    # scheduler = BackgroundScheduler()
    # This will get you a BackgroundScheduler with a MemoryJobStore named
    # “default” and a ThreadPoolExecutor named “default” with a default
    # maximum thread count of 10.

    # Lets customize the scheduler a little bit lets keep the default
    # MemoryJobStore but define a ProcessPoolExecutor
    jobstores = {
        "default": SQLAlchemyJobStore(
            url="sqlite:////Users/pemartin/Projects/file-provenance-tracker/src/jobstore.sqlite"  # noqa: E501
        )
    }
    executors = {
        "default": ThreadPoolExecutor(8),
    }
    job_defaults = {"coalesce": False, "max_instances": 3}
    return BackgroundScheduler(
        jobstores=jobstores, executors=executors, job_defaults=job_defaults
    )


def graph_components_generator(number_of_tasks):  # pylint: disable=too-many-locals
    """! This function will generate the graph of the entire project

    Args:
        number_of_tasks (int): A number describing the number of
          tasks to be added

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
            _, col2, col4, col5, col6, col7, col8, col9 = st.columns(
                [1, 2, 2, 2, 2, 2, 2, 2]
            )
            # stage_type = col1.selectbox("Select node type", ["task"],
            # key=f"stage_{i}")

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
            )  # noqa: E501

            command = col6.text_input(
                f"Command for task {i}", key=f"cmd_{i}", placeholder="Command"
            )

            message = col7.text_input(
                f"Message for task {i}", key=f"msg_{i}", placeholder="Message"
            )

            pce = col8.text_input(
                f"PCE for task {i}",
                key=f"pce_{i}",
                placeholder="Enter a PCE, it can be any series string without dashes",
            )

            subworkflow = col9.text_input(
                f"Subworkflow for task {i}", key=f"wrkf_{i}", placeholder="Subworkflow"
            )

            inputs = []
            outputs = []
            for item in inputs_grp.split(","):
                inps_expanded = utilities.file_name_expansion(item)

                if (
                    len(item.rstrip()) == 0
                ):  # if there is no file (or there is an empty file)
                    # stop the execution
                    st.stop()

                inputs.extend(inps_expanded)

            for item in outputs_grp.split(","):
                outps_expanded = utilities.file_name_expansion(item)

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
            diff_set = set(node1[1]["outputs"]).intersection(
                set(node2[1]["inputs"])
            )  # noqa: E501
            if diff_set:
                edges.append((node1[0], node2[0]))

    return nodes, edges


def plot_graph(plot):
    """! Function to generate a bokeh chart

    Args:
        plot (plot): A networkx plot
    """
    st.bokeh_chart(plot, use_container_width=True)


def generate_code(graph):
    """Generate workflow code based on the provided graph.

    Args:
        graph (DiGraph): The graph containing the workflow information.

    Returns:
        str: The generated workflow code as a string.
    """
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

    workflows = nx.get_node_attributes(graph, "subworkflow").values()
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

        task_nodes = [n for n, v in graph.nodes(data=True) if v["subworkflow"] == flow]

        body_list = []
        for task in task_nodes:
            # inputs  = graph.predecessors(task)
            # outputs = graph.successors(task)
            inputs = graph.nodes[task]["inputs"][0].split(",")
            outputs = graph.nodes[task]["outputs"][0].split(",")
            command = graph.nodes[task]["command"]

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
        with open(path, "w", encoding="utf-8") as file_abs:
            for node in nodes:
                file_abs.writelines(
                    f"T<>{node[0]}<>{','.join(node[1]['inputs'])}<>{','.join(node[1]['outputs'])}<>{node[1]['command']}<>{node[1]['PCE']}<>{node[1]['subworkflow']}<>{node[1]['message']}\n"  # noqa: E501
                )

        # kwargs["graph"].graph_export(kwargs["filename"])
    except ValueError as exception_graph:
        st.sidebar.text(f"{exception_graph}")


def match_graphs(provenance_ds_path,
                 gdb_abstract: nx.DiGraph,
                 ds_branch: str) -> nx.DiGraph:
    """! Function to match the graphs loaded with Streamlit interface

    Args:
        provenance_ds_path (str)`: The path to the provenance dataset
        gdb_abstract (graph): An abstract graph
    """
    node_mapping = {}
    repo = git.Repo(provenance_ds_path)
    branch = repo.heads[ds_branch]
    branch.checkout()
    with open(
        f"{provenance_ds_path}/tf.csv", "r", encoding="utf-8"
    ) as translation_file:
        reader = csv.reader(translation_file)
        for row in reader:
            node_mapping[row[0]] = f"{provenance_ds_path}{row[1]}"

    if utilities.exists_case_sensitive(provenance_ds_path):
        nodes_provenance, edges_provenance = graphs.prov_scan(
            provenance_ds_path, ds_branch
        )
        gdb_provenance = nx.DiGraph()
        gdb_provenance.add_nodes_from(nodes_provenance)
        gdb_provenance.add_edges_from(edges_provenance)

        gdb_abstract = match.graph_remap_command_task(gdb_abstract, node_mapping)
        gdb_abstract = match.graph_id_relabel(gdb_abstract, node_mapping)
        gdb_difference = match.graph_diff_tasks(gdb_abstract, gdb_provenance)

        if gdb_difference:
            graph_plot_diff = graphs.graph_object_plot_task(gdb_abstract)
            plot_graph(graph_plot_diff)

            next_nodes_requirements = match.next_nodes_run(gdb_difference)

        if "next_nodes_req" not in st.session_state:
            st.session_state["next_nodes_req"] = next_nodes_requirements

    else:
        st.warning(f"Path {provenance_ds_path} does not exist.")
        st.stop()

    return gdb_difference


def run_pending_nodes_scheduler(
    scheduler_instance: BackgroundScheduler,
    provenance_ds_path: str,
    gdb_difference: nx.DiGraph,
    branch: str
):  # pylint: disable=too-many-locals
    """! Given a graph and the list of nodes (and requirements i.e. inputs)
    compute the task with APScheduler

    Args:
        gdb_difference (graph): An abstract graph
    """
    inputs_dict = {}
    outputs_dict = {}
    inputs = []

    # we need to use the translation file so the nodes in the difference tree have the
    # file names instead of the abstract names. From the nodes we can extract the
    # list of inputs and outputs for the job that is going to run
    node_mapping = {}
    with open(
        f"{provenance_ds_path}/tf.csv", "r", encoding="utf-8"
    ) as translation_file:
        reader = csv.reader(translation_file)
        for row in reader:
            node_mapping[row[0]] = f"{provenance_ds_path}{row[1]}"

    gdb_difference = match.graph_id_relabel(gdb_difference, node_mapping)

    try:
        next_nodes = st.session_state["next_nodes_req"]
        for item in next_nodes:
            for predecessors in gdb_difference.predecessors(item):
                inputs_dict[predecessors] = gdb_difference.nodes[predecessors]
                inputs.append(gdb_difference.nodes[predecessors])

            for successors in gdb_difference.successors(item):
                outputs_dict[successors] = gdb_difference.nodes[successors]

            inputs = list(inputs_dict.keys())
            outputs = list(outputs_dict.keys())
            dataset = utilities.get_git_root(os.path.dirname(inputs[0]))
            command = gdb_difference.nodes[item]["cmd"]
            message = "test"

            scheduler_instance.add_job(
                utilities.job_submit,
                args=[dataset, branch, inputs, outputs, message, command],
            )

    except ValueError as err:  # pylint: disable = bare-except
        st.warning(
            f"No provance graph has been matched to this abstract graph, match one first {err}"  # noqa: E501
        )


def graph_object_plot(graph_input, fcolor="node_color"):
    """! Utility to generate a plot for a networkx graph
    Args:
        graph_nx (graph): A networkx graph
    Returns:
        plot: A graphviz figure to be plotted with bokeh
    """
    # The next two lines are to fix an issue with bokeh 3.3.0 if using bokeh 2.4.3
    #  these can be removed
    mapping = dict((n, i) for i, n in enumerate(graph_input.nodes))
    graph_relabeled = nx.relabel_nodes(graph_input, mapping=mapping)
    nx.set_node_attributes(
        graph_relabeled, "grey", name=fcolor
    )  # adding grey color at initialization
    graph_layout = graphviz_layout(
        graph_relabeled, prog="dot", root=None, args="-Gnodesep=1000 -Grankdir=TB"
    )
    graph = from_networkx(graph_relabeled, graph_layout)
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
            ("PCE", "@PCE"),
        ]
    )
    plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())
    graph.node_renderer.glyph = Circle(size=20, fill_color=fcolor)
    plot.renderers.append(graph)  # pylint: disable=no-member
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
    plot.renderers.append(labels)  # pylint: disable=no-member
    return plot


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Added argument parser to parse a file with a workflow
    # from a text file, the text file
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

    # we initialize the scheduler
    scheduler = scheduler_configuration()
    scheduler.start()

    next_nodes_req = []  # type: ignore

    node_list = None  # pylint: disable=invalid-name
    edge_list = None  # pylint: disable=invalid-name

    if args.agraph:
        node_list, edge_list = graphs.gcg_processing_tasks(args.agraph)

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

    graph_plot_abstract = graphs.graph_object_plot_abstract(gdb)
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
    provenance_graph_path = st.sidebar.text_input(
        """Path to the dataset
                                                  with provenance"""
    )

    if st.sidebar.button("Generate code"):
        code_workflow = generate_code(gdb)
        st.text_area("Prefect code", code_workflow)

    # here we add a button to record the abstract graph as provenance
    if st.sidebar.button("Record as provenance"):
        graphs.write_network_text(
            gdb, with_labels=True, vertical_chains=True, ascii_only=True
        )
        graphs.abs2prov(gdb, provenance_graph_path, "abstract")

    if utilities.exists_case_sensitive(provenance_graph_path):
        branches_project = utilities.get_branches(provenance_graph_path)
        branch_select = st.sidebar.selectbox("Branches", branches_project)
        match_button = st.sidebar.button("Match")

        if match_button:
            match_graphs(provenance_graph_path, gdb, branch_select)
        run_next_button = st.sidebar.button("Run pending nodes")
        if run_next_button:
            run_pending_nodes_scheduler(
                scheduler, provenance_graph_path, gdb, branch_select
            )
