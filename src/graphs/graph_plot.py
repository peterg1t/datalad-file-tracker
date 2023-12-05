"""A class for graph plotting."""
import networkx as nx
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


def graph_object_plot_task(graph_input, fcolour="node_color"):
    """! Utility to generate a plot for a networkx graph
    Args:
        graph_nx (graph): A networkx graph
    Returns:
        plot: A graphviz figure to be plotted with bokeh
    """
    # The next two lines are to fix an issue with bokeh 3.3.0 if
    # using bokeh 2.4.3 these can be removed
    mapping = dict((n, i) for i, n in enumerate(graph_input.nodes))
    relabeled_graph = nx.relabel_nodes(graph_input, mapping=mapping)

    for _, attrs in relabeled_graph.nodes(data=True):
        print(attrs)
        attrs["name"] = attrs["description"]

    # adding grey color at initialization
    # nx.set_node_attributes(relabeled_graph, "grey", name=fcolour)

    graph_layout = graphviz_layout(
        relabeled_graph, prog="dot", root=None, args="-Gnodesep=1000 -Grankdir=TB"
    )

    graph = from_networkx(relabeled_graph, graph_layout)

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
            ("name", "@name"),
            ("label", "@label"),
            ("status", "@status"),
            ("node_color", "@node_color"),
            ("ID", "@ID"),
        ]
    )
    plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())
    graph.node_renderer.glyph = Circle(size=20, fill_color=fcolour)
    plot.renderers.append(graph)  # pylint: disable=no-member

    x_coord, y_coord = zip(
        *graph.layout_provider.graph_layout.values()  # pylint: disable=no-member
    )
    node_labels = nx.get_node_attributes(relabeled_graph, "name")
    node_names = list(node_labels.values())

    source = ColumnDataSource({"x": x_coord, "y": y_coord, "name": node_names})

    labels = LabelSet(
        x="x",
        y="y",
        text="name",
        source=source,
        background_fill_color="white",
        text_align="center",
        y_offset=11,
    )

    plot.renderers.append(labels)  # pylint: disable=no-member

    return plot


def graph_object_plot_abstract(graph_input, fcolour="node_color"):
    """! Utility to generate a plot for a networkx graph
    Args:
        graph_nx (graph): A networkx graph
    Returns:
        plot: A graphviz figure to be plotted with bokeh
    """
    # The next two lines are to fix an issue with bokeh 3.3.0
    # if using bokeh 2.4.3 these can be removed
    mapping = dict((n, i) for i, n in enumerate(graph_input.nodes))
    relabeled_graph = nx.relabel_nodes(graph_input, mapping=mapping)

    # adding grey color at initialization
    # nx.set_node_attributes(relabeled_graph, "grey", name=fcolour)

    graph_layout = graphviz_layout(
        relabeled_graph, prog="dot", root=None, args="-Gnodesep=1000 -Grankdir=TB"
    )

    graph = from_networkx(relabeled_graph, graph_layout)

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
            ("name", "@name"),
            ("label", "@label"),
            ("status", "@status"),
            ("node_color", "@node_color"),
            ("ID", "@ID"),
        ]
    )
    plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())
    graph.node_renderer.glyph = Circle(size=20, fill_color=fcolour)
    plot.renderers.append(graph)  # pylint: disable=no-member

    x_coord, y_coord = zip(
        *graph.layout_provider.graph_layout.values()  # pylint: disable=no-member
    )
    node_labels = nx.get_node_attributes(relabeled_graph, "name")
    node_names = list(node_labels.values())

    source = ColumnDataSource({"x": x_coord, "y": y_coord, "name": node_names})

    labels = LabelSet(
        x="x",
        y="y",
        text="name",
        source=source,
        background_fill_color="white",
        text_align="center",
        y_offset=11,
    )

    plot.renderers.append(labels)  # pylint: disable=no-member

    return plot


def graph_object_plot_provenance(graph_input, fcolour="node_color"):
    """! Utility to generate a plot for a networkx graph
    Args:
        graph_nx (graph): A networkx graph
    Returns:
        plot: A graphviz figure to be plotted with bokeh
    """
    # The next two lines are to fix an issue with bokeh 3.3.0 if using
    # bokeh 2.4.3 these can be removed
    mapping = dict((n, i) for i, n in enumerate(graph_input.nodes))
    relabeled_graph = nx.relabel_nodes(graph_input, mapping=mapping)
    for _, attrs in relabeled_graph.nodes(data=True):
        attrs["ID"] = attrs["ID"].split(",")[-1]
        print(attrs["ID"].split(",")[-1])

    print(relabeled_graph.nodes(data=True))

    # nx.set_node_attributes(
    #     relabeled_graph, "grey", name=fcolour
    # )  # adding grey color at initialization

    graph_layout = graphviz_layout(
        relabeled_graph, prog="dot", root=None, args="-Gnodesep=1000 -Grankdir=TB"
    )

    graph = from_networkx(relabeled_graph, graph_layout)

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
            ("path", "@path"),
            ("command", "@command"),
            ("commit", "@commit"),
            ("date", "@date"),
            ("ID", "@ID"),
        ]
    )
    plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())
    graph.node_renderer.glyph = Circle(size=20, fill_color=fcolour)
    plot.renderers.append(graph)  # pylint: disable=no-member

    x_coord, y_coord = zip(
        *graph.layout_provider.graph_layout.values()  # pylint: disable=no-member
    )
    node_labels = nx.get_node_attributes(relabeled_graph, "ID")
    node_names = list(node_labels.values())

    source = ColumnDataSource({"x": x_coord, "y": y_coord, "ID": node_names})

    labels = LabelSet(
        x="x",
        y="y",
        text="ID",
        source=source,
        background_fill_color="white",
        text_align="center",
        y_offset=11,
    )

    plot.renderers.append(labels)  # pylint: disable=no-member

    return plot
