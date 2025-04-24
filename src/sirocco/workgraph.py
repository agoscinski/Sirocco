from __future__ import annotations

import uuid 
import io
import functools
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeAlias

import aiida.common
import aiida.orm
import aiida_workgraph.tasks.factory.shelljob_task # type: ignore[import-untyped]

from aiida.common.exceptions import NotExistent
from aiida_workgraph import WorkGraph

from sirocco import core

if TYPE_CHECKING:
    from aiida_workgraph.socket import TaskSocket  # type: ignore[import-untyped]
    from aiida_workgraph.sockets.builtins import SocketAny

    WorkgraphDataNode: TypeAlias = aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData


# This is a workaround required when splitting the initialization of the task and its linked nodes Merging this into
# aiida-workgraph properly would require significant changes see issues
# https://github.com/aiidateam/aiida-workgraph/issues/168 The function is a copy of the original function in
# aiida-workgraph. The modifications are marked by comments.
#def _prepare_for_shell_task(inputs: dict) -> dict:
#    """Prepare the inputs for ShellJob"""
#    from aiida_shell.launch import prepare_shell_job_inputs
#    import inspect
#
#    # Retrieve the signature of `prepare_shell_job_inputs` to determine expected input parameters.
#    signature = inspect.signature(prepare_shell_job_inputs)
#    aiida_shell_input_keys = signature.parameters.keys()
#
#    # Iterate over all WorkGraph `inputs`, and extract the ones which are expected by `prepare_shell_job_inputs`
#    inputs_aiida_shell_subset = {
#        key: inputs[key] for key in inputs.keys() if key in aiida_shell_input_keys
#    }
#    # if parser in inputs, and the parser is a dict
#    parser = inputs_aiida_shell_subset.get("parser", None)
#    if isinstance(parser, dict):
#        inputs_aiida_shell_subset["parser"] = NodeExecutor(**parser).executor
#
#    try:
#        aiida_shell_inputs = prepare_shell_job_inputs(**inputs_aiida_shell_subset)
#    except ValueError:
#        raise
#
#    # We need to remove the original input-keys, as they might be offending for the call to `launch_shell_job`
#    # E.g., `inputs` originally can contain `command`, which gets, however, transformed to #
#    # `code` by `prepare_shell_job_inputs`
#    for key in inputs_aiida_shell_subset.keys():
#        inputs.pop(key)
#
#    # Finally, we update the original `inputs` with the modified ones from the call to `prepare_shell_job_inputs`
#    inputs = {**inputs, **aiida_shell_inputs}
#
#    inputs.setdefault("metadata", {})
#
#    # Workaround starts here
#    # This part is part of the workaround. We need to manually add the outputs from the task.
#    # Because kwargs are not populated with outputs
#    #breakpoint()
#    #default_outputs = {"remote_folder", "remote_stash", "retrieved", "_outputs", "_wait", "stdout", "stderr"}
#    #task_outputs = set(task["outputs"].keys())
#    #task_outputs = task_outputs.union(set(inputs.pop("outputs", [])))
#    #missing_outputs = task_outputs.difference(default_outputs)
#    #inputs["outputs"] = list(missing_outputs)
#    # Workaround ends here
#
#    return inputs
#
#
#
#aiida_workgraph.tasks.factory.shelljob_task.prepare_for_shell_task = _prepare_for_shell_task


class AiidaWorkGraph:
    def __init__(self, core_workflow: core.Workflow):
        # the core workflow that unrolled the time constraints for the whole graph
        self._core_workflow = core_workflow

        self._validate_workflow()

        self._workgraph = WorkGraph(core_workflow.name)

        # stores the input data available on initialization
        self._aiida_data_nodes: dict[str, WorkgraphDataNode] = {}
        # stores the outputs sockets of tasks
        self._aiida_socket_nodes: dict[str, TaskSocket] = {}
        self._aiida_task_nodes: dict[str, aiida_workgraph.Task] = {}

        # create input data nodes
        for data in self._core_workflow.data:
            if isinstance(data, core.AvailableData):
                # TODO here we do not have the date available that is required for init
                #      need to iterater first cycle
                # TODO check why this did not already make the tests crash
                #      should have been caught earlier
                self._add_aiida_input_data_node(data)

        # create workgraph task nodes and output sockets
        for task in self._core_workflow.tasks:
            self.create_task_node(task)
            # Create and link corresponding output sockets
            for port, output in task.outputs.items():
                self._link_output_node_to_task(task, port, output)

        # link input nodes to workgraph tasks
        for task in self._core_workflow.tasks:
            for port, input_ in task.input_data_nodes():
                self._link_input_node_to_task(task, port, input_)

        # set shelljob arguments
        for task in self._core_workflow.tasks:
            if isinstance(task, core.ShellTask):
                self._set_shelljob_arguments(task)

        # link wait on to workgraph tasks
        for task in self._core_workflow.tasks:
            self._link_wait_on_to_task(task)

    def _validate_workflow(self):
        """Checks if the core workflow uses valid AiiDA names for its tasks and data."""
        for task in self._core_workflow.tasks:
            try:
                aiida.common.validate_link_label(task.name)
            except ValueError as exception:
                msg = f"Raised error when validating task name '{task.name}': {exception.args[0]}"
                raise ValueError(msg) from exception
            for _, input_ in task.input_data_nodes():
                try:
                    aiida.common.validate_link_label(input_.name)
                except ValueError as exception:
                    msg = f"Raised error when validating input name '{input_.name}': {exception.args[0]}"
                    raise ValueError(msg) from exception
            for output in task.outputs.values():
                try:
                    aiida.common.validate_link_label(output.name)
                except ValueError as exception:
                    msg = f"Raised error when validating output name '{output.name}': {exception.args[0]}"
                    raise ValueError(msg) from exception

    @staticmethod
    def replace_invalid_chars_in_label(label: str) -> str:
        """Replaces chars in the label that are invalid for AiiDA.

        The invalid chars ["-", " ", ":", "."] are replaced with underscores.
        """
        invalid_chars = ["-", " ", ":", "."]
        for invalid_char in invalid_chars:
            label = label.replace(invalid_char, "_")
        return label

    @classmethod
    def get_aiida_label_from_graph_item(cls, obj: core.GraphItem) -> str:
        """Returns a unique AiiDA label for the given graph item.

        The graph item object is uniquely determined by its name and its coordinates. There is the possibility that
        through the replacement of invalid chars in the coordinates duplication can happen but it is unlikely.
        """
        
        return cls.replace_invalid_chars_in_label(
            f"{obj.name}" + "__".join(f"_{key}_{value}" for key, value in obj.coordinates.items())
        )

    @classmethod
    def get_aiida_label_from_available_data(cls, obj: core.GraphItem) -> str:
        """Returns a unique AiiDA label for the given available data.

        TODO adapt doc, does ignore coordinates as it is not generated
             For available data we ignore coordinates, because the only coordinate
             they support is date and this one is just expressing their usage
             the datetime is just the usage time but not when they are generated
             since they are there since the beginning. Since we do not now when available
             data nodes are used, the date can be arbitrary
             Somehow need to make this logic more explicit in core

        The graph item object is uniquely determined by its name and its coordinates. There is the possibility that
        through the replacement of invalid chars in the coordinates duplication can happen but it is unlikely.
        """
        
        return cls.replace_invalid_chars_in_label(
            f"{obj.name}"
        )
    @staticmethod
    def split_cmd_arg(command_line: str) -> tuple[str, str]:
        split = command_line.split(sep=" ", maxsplit=1)
        if len(split) == 1:
            return command_line, ""
        return split[0], split[1]

    @classmethod
    def label_placeholder(cls, data: core.Data) -> str:
        return f"{{{cls.get_aiida_label_from_graph_item(data)}}}"

    # TODO reimplement both methods below with dispatching

    # TODO name confusing because it is not from core but from the already created workflow
    def data_from_core(self, core_available_data: core.AvailableData) -> WorkgraphDataNode:
        return self._aiida_data_nodes[self.get_aiida_label_from_available_data(core_available_data)]

    # TODO implement this
    #def data_to_core(self, core_available_data: core.AvailableData) -> WorkgraphDataNode:
    #    return self._aiida_data_nodes[self.get_aiida_label_from_graph_item(core_available_data)]

    def socket_from_core(self, core_generated_data: core.GeneratedData) -> TaskSocket:
        return self._aiida_socket_nodes[self.get_aiida_label_from_graph_item(core_generated_data)]

    def task_from_core(self, core_task: core.Task) -> aiida_workgraph.Task:
        return self._aiida_task_nodes[self.get_aiida_label_from_graph_item(core_task)]

    def _add_available_data(self):
        """Adds the available data on initialization to the workgraph"""
        for data in self._core_workflow.data:
            if isinstance(data, core.AvailableData):
                self._add_aiida_input_data_node(data)

    def _add_aiida_input_data_node(self, data: core.Data):
        """
        Create an `aiida.orm.Data` instance from the provided graph item.
        """
        label = self.get_aiida_label_from_graph_item(data)
        data_full_path = str(data.src) if data.src.is_absolute() else self._core_workflow.config_rootdir / data.src

        if data.computer is not None:
            try:
                computer = aiida.orm.load_computer(data.computer)
            except NotExistent as err:
                msg = f"Could not find computer {data.computer!r} for input {data}."
                raise ValueError(msg) from err
            self._aiida_data_nodes[label] = aiida.orm.RemoteData(remote_path=str(data.src), label=label, computer=computer)
        elif data.type == "file":
            self._aiida_data_nodes[label] = aiida.orm.SinglefileData(label=label, file=data_full_path)
        elif data.type == "dir":
            self._aiida_data_nodes[label] = aiida.orm.FolderData(label=label, tree=data_full_path)
        else:
            msg = f"Data type {data.type!r} not supported. Please use 'file' or 'dir'."
            raise ValueError(msg)

    @functools.singledispatchmethod
    def create_task_node(self, task: core.Task):
        """dispatch creating task nodes based on task type"""

        if isinstance(task, core.IconTask):
            msg = "method not implemented yet for Icon tasks"
        else:
            msg = f"method not implemented for task type {type(task)}"
        raise NotImplementedError(msg)

    @create_task_node.register
    def _create_shell_task_node(self, task: core.ShellTask):
        label = self.get_aiida_label_from_graph_item(task)
        # Split command line between command and arguments (this is required by aiida internals)
        cmd, _ = self.split_cmd_arg(task.command)
        cmd_path = Path(cmd)
        # FIXME: https://github.com/C2SM/Sirocco/issues/127
        if cmd_path.is_absolute():
            command = str(cmd_path)
        else:
            if task.src is None:
                msg = "src must be specified when command path is relative"
                raise ValueError(msg)
            command = str((task.config_rootdir / task.src).parent / cmd_path)

        # metadata
        metadata: dict[str, Any] = {}
        ## Source file
        env_source_paths = [
            env_source_path
            if (env_source_path := Path(env_source_file)).is_absolute()
            else (task.config_rootdir / env_source_path)
            for env_source_file in task.env_source_files
        ]
        prepend_text = "\n".join([f"source {env_source_path}" for env_source_path in env_source_paths])
        metadata["options"] = {"prepend_text": prepend_text}

        ## computer
        if task.computer is not None:
            try:
                metadata["computer"] = aiida.orm.load_computer(task.computer)
            except NotExistent as err:
                msg = f"Could not find computer {task.computer} for task {task}."
                raise ValueError(msg) from err

        # NOTE: We don't pass the `nodes` dictionary here, as then we would need to have the sockets available when
        # we create the task. Instead, they are being updated via the WG internals when linking inputs/outputs to
        # tasks
        workgraph_task = self._workgraph.add_task(
            "workgraph.shelljob",
            name=label,
            command=command,
            arguments="",
            outputs=[],
            metadata=metadata,
        )

        self._aiida_task_nodes[label] = workgraph_task


    def _get_localhost_computer(self) -> aiida.orm.Computer:
        """Return a :class:`aiida.orm.computers.Computer` instance.

        The database is queried for an existing computer with the given label. If it exists, it means it was probably
        created by this fixture in a previous call and it is simply returned. Otherwise a new instance is created.
        Note that the computer is not explicitly configured, unless ``configure_kwargs`` are specified.

        :param label: The computer label. If not specified, a random UUID4 is used.
        :param minimum_job_poll_interval: The default minimum job poll interval to set.
        :param configuration_kwargs: Optional keyword arguments that, if defined, are used to configure the computer
            by calling :meth:`aiida.orm.computers.Computer.configure`.
        :param kwargs: Optional keyword arguments that are passed to the :class:`aiida.orm.computers.Computer`
            constructor if a computer with the given label doesn't already exist.
        :return: A stored computer instance.
        """
        label="localhost"
        import tempfile

        try:
            computer = aiida.orm.Computer.collection.get(label=label)
        except NotExistent:
            computer = aiida.orm.Computer(
                label=label,
                description="Local computer",
                hostname='localhost',
                workdir=tempfile.mkdtemp(), # TODO what is put here?
                transport_type='core.local',
                scheduler_type='core.direct',
            )
            computer.store()
            computer.set_minimum_job_poll_interval(5) # TODO why in aiida-core tests this is 0? Is this ignored for core.direct?
            computer.set_default_mpiprocs_per_machine(1) # TODO I think user args we set this when submitting, also aiida has some automatic detection need to check this

        return computer

    @create_task_node.register
    def _create_icon_task_node(self, task: core.IconTask):
        IconCalculation = aiida.plugins.CalculationFactory('icon.icon')
        task_label = self.get_aiida_label_from_graph_item(task)

        if task.computer is None:
            computer = self._get_localhost_computer()
        else:
            try:
                computer = aiida.orm.Computer.collection.get(label=task.computer)
            except NotExistent:
                raise ValueError(f"Could not find computer {task.computer!r} in AiiDA database. One needs to create and configure the computer before running a workflow.")


        label_uuid = str(uuid.uuid4())
        icon_code = aiida.orm.InstalledCode(
            label=f"icon-{label_uuid}", # PR COMMENT create always new code
            description="aiida_icon",
            default_calc_job_plugin="icon.icon",
            computer=computer,
            filepath_executable=str(task.src),
            with_mpi=True # TODO need to install icon with mpi so we do not determine this parameter smartly
        ).store()

        #builder = icon_code.get_builder()
        #builder.master_namelist = aiida.orm.SinglefileData(inputs_path / "icon_master.namelist")
        #builder.model_namelist = aiida.orm.SinglefileData(inputs_path / "model.namelist")
        #builder.dynamics_grid_file = make_remote(remote_path=str(inputs_path / "icon_grid_simple.nc"))
        #builder.ecrad_data = make_remote(remote_path=str(inputs_path / "ecrad_data"))
        #builder.rrtmg_sw = make_remote(remote_path=str(inputs_path / "rrtmg_sw.nc"))
        #builder.cloud_opt_props = make_remote(remote_path=str(inputs_path / "ECHAM6_CldOptProps.nc"))
        #builder.dmin_wetgrowth_lookup = make_remote(remote_path=str(inputs_path / "dmin_wetgrowth_lookup.nc"))

        builder = IconCalculation.get_builder()
        builder.code = icon_code


        # This needs to be moved
        #setattr(workgraph_task.inputs, f"{port}", self.data_from_core(input_))

        task.update_namelists_from_workflow() # updates datetime
        with io.StringIO() as buffer:
            task.master_namelist.content.write(buffer)
            buffer.seek(0)
            builder.master_namelist = aiida.orm.SinglefileData(buffer, task.master_namelist.name)

        with io.StringIO() as buffer:
            task.model_namelist.content.write(buffer)
            buffer.seek(0)
            builder.model_namelist = aiida.orm.SinglefileData(buffer, task.model_namelist.name)
        icon_task = self._workgraph.add_task(builder)
        self._aiida_task_nodes[task_label] = icon_task

        # TODO do we update model namelist internally at some point?
        #name = task.core_model_namelist.name
        #output_stream = io.StringIO()
        #suffix = ("_".join([str(p) for p in task.coordinates.values()])).replace(" ", "_")
        #filename = name + "_" + suffix
        #icon_task.inputs.model_namelist.value = aiida.orm.SinglefileData.from_string(task.core_model_namelist.write(output_stream).getvalue(), filename)

        #data_full_path = data.src if data_path.is_absolute() else self._core_workflow.config_rootdir / data_path
        #icon_task.inputs.model_namelist.value = aiida.orm.SinglefileData(self.get_data_full_path(task.model_namelist.path))


    @functools.singledispatchmethod
    def _link_output_node_to_task(self, task: core.Task, port: str, output: core.Data):  # noqa: ARG002
        """Dispatch linking input to task based on task type."""

        msg = f"method not implemented for task type {type(task)}"
        raise NotImplementedError(msg)

    @_link_output_node_to_task.register
    def _link_output_node_to_shell_task(self, task: core.ShellTask, _: str, output: core.Data):
        """Links the output to the workgraph task."""

        workgraph_task = self.task_from_core(task)
        output_label = self.get_aiida_label_from_graph_item(output)
        output_socket = workgraph_task.add_output("workgraph.any", output.src)
        self._aiida_socket_nodes[output_label] = output_socket

    @_link_output_node_to_task.register
    def _link_output_node_to_icon_task(self, task: core.IconTask, port: str, output: core.Data):
        workgraph_task = self.task_from_core(task)
        output_label = self.get_aiida_label_from_graph_item(output)
        output_socket = workgraph_task.outputs._sockets.get(port)
        self._aiida_socket_nodes[output_label] = output_socket

    @functools.singledispatchmethod
    def _link_input_node_to_task(self, task: core.Task, port: str, input_: core.Data):  # noqa: ARG002
        """ "Dispatch linking input to task based on task type"""

        msg = f"method not implemented for task type {type(task)}"
        raise NotImplementedError(msg)

    @_link_input_node_to_task.register
    def _link_input_node_to_shell_task(self, task: core.ShellTask, _: str, input_: core.Data):
        """Links the input to the workgraph shell task."""

        workgraph_task = self.task_from_core(task)
        input_label = self.get_aiida_label_from_graph_item(input_)
        workgraph_task.add_input("workgraph.any", f"nodes.{input_label}")

        # resolve data
        if isinstance(input_, core.AvailableData):
            if not hasattr(workgraph_task.inputs.nodes, f"{input_label}"):
                msg = f"Socket {input_label!r} was not found in workgraph. Please contact a developer."
                raise ValueError(msg)
            socket = getattr(workgraph_task.inputs.nodes, f"{input_label}")
            socket.value = self.data_from_core(input_)
        elif isinstance(input_, core.GeneratedData):
            self._workgraph.add_link(self.socket_from_core(input_), workgraph_task.inputs[f"nodes.{input_label}"])
        else:
            raise TypeError

    @_link_input_node_to_task.register
    def _link_input_node_to_icon_task(self, task: core.IconTask, port: str, input_: core.Data):
        """Links the input to the workgraph shell task."""

        workgraph_task = self.task_from_core(task)

        # resolve data
        if isinstance(input_, core.AvailableData):
            setattr(workgraph_task.inputs, f"{port}", self.data_from_core(input_))
        elif isinstance(input_, core.GeneratedData):
            setattr(workgraph_task.inputs, f"{port}", self.socket_from_core(input_))
        else:
            raise TypeError

    def _link_wait_on_to_task(self, task: core.Task):
        """link wait on tasks to workgraph task"""

        self.task_from_core(task).wait = [self.task_from_core(wt) for wt in task.wait_on]

    def _set_shelljob_arguments(self, task: core.ShellTask):
        """set AiiDA ShellJob arguments by replacing port placeholders by aiida labels"""

        workgraph_task = self.task_from_core(task)
        workgraph_task_arguments: SocketAny = workgraph_task.inputs.arguments
        if workgraph_task_arguments is None:
            msg = (
                f"Workgraph task {workgraph_task.name!r} did not initialize arguments nodes in the workgraph "
                f"before linking. This is a bug in the code, please contact developers."
            )
            raise ValueError(msg)

        input_labels = {port: list(map(self.label_placeholder, task.inputs[port])) for port in task.inputs}
        _, arguments = self.split_cmd_arg(task.resolve_ports(input_labels))
        workgraph_task_arguments.value = arguments

    def run(
        self,
        inputs: None | dict[str, Any] = None,
        metadata: None | dict[str, Any] = None,
    ) -> aiida.orm.Node:
        self._workgraph.run(inputs=inputs, metadata=metadata)
        if (output_node := self._workgraph.process) is None:
            raise RuntimeError("Something went wrong when running workgraph. Please contact a developer.")
        return output_node

    def submit(
        self,
        *,
        inputs: None | dict[str, Any] = None,
        wait: bool = False,
        timeout: int = 60,
        metadata: None | dict[str, Any] = None,
    ) -> aiida.orm.Node:
        self._workgraph.submit(inputs=inputs, wait=wait, timeout=timeout, metadata=metadata)
        if (output_node := self._workgraph.process) is None:
            raise RuntimeError("Something went wrong when running workgraph. Please contact a developer.")
        return output_node
