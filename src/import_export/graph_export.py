"""This file will contain the export methods"""
import streamlit as st


def export_graph(**kwargs):
    """! This function will export the graph to Pedro's notation and
    throws an exception to streamlit if there is some error
    """
    try:
        nodes = kwargs["graph"].nodes(data=True)
        with open(kwargs["filename"], "w", encoding="utf-8") as file_abs:
            for node in nodes:
                if "cmd" in node[1]:
                    file_abs.writelines(
                        f"{node[1]['type'][0].upper()}<>{node[0]}<>{','.join(node[1]['predecesor'])}<>{node[1]['cmd']}<>{node[1]['workflow']}\n"  # noqa: E501
                    )
                else:
                    file_abs.writelines(
                        f"{node[1]['type'][0].upper()}<>{node[0]}<>{','.join(node[1]['predecesor'])}\n"  # noqa: E501
                    )
        # kwargs["graph"].graph_export(kwargs["filename"])
    except ValueError as exception_graph:
        st.sidebar.text(f"{exception_graph}")
