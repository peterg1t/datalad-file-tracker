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


def graph_plot(graph_nx):
    """Utility to generate a plot for a networkx graph

    Args:
        graph_nx (graph): A networkx graph

    Returns:
        plot: A graphviz figure to be plotted with bokeh
    """
    gl = graphviz_layout(
        graph_nx, prog="dot", root=None, args="-Gnodesep=1000 -Grankdir=TB"
    )
    graph = from_networkx(graph_nx, gl)

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
            ("literal_name", "@literal_name"),
            ("status", "@status"),
        ]
    )
    plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())

    graph.node_renderer.glyph = Circle(size=20, fill_color="node_color")
    plot.renderers.append(graph)

    x, y = zip(*graph.layout_provider.graph_layout.values())
    node_labels = nx.get_node_attributes(graph_nx, "literal_name")

    fn = list(node_labels.values())
    source = ColumnDataSource({"x": x, "y": y, "basename": fn})
    labels = LabelSet(
        x="x",
        y="y",
        text="basename",
        source=source,
        background_fill_color="white",
        text_align="center",
        y_offset=11,
    )
    print("labels", labels)
    plot.renderers.append(labels)

    return plot
