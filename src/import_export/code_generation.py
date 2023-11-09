"""This module will generate prefect code."""
import ast


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