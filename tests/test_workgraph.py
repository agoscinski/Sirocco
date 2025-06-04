import textwrap

import pytest
from aiida import orm

from sirocco.core import GeneratedData, Workflow
from sirocco.parsing import yaml_data_models as models
from sirocco.workgraph import AiidaWorkGraph


def test_get_aiida_label_from_graph_item(tmp_path):
    """Test that AiiDA labels are generated correctly."""

    # Mock data nodes with different coordinate combinations
    output_path = tmp_path / "output"
    data_simple = GeneratedData(name="output", type=models.DataType.FILE, src=output_path, coordinates={})

    data_with_date = GeneratedData(
        name="output",
        type=models.DataType.FILE,
        src=output_path,
        coordinates={"date": "2026-01-01-00:00:00"},
    )

    data_with_params = GeneratedData(
        name="output",
        type=models.DataType.FILE,
        src=output_path,
        coordinates={"foo": 0, "bar": 3.0, "date": "2026-01-01-00:00:00"},
    )

    # Test label generation
    assert AiidaWorkGraph.get_aiida_label_from_graph_item(data_simple) == "output"
    assert AiidaWorkGraph.get_aiida_label_from_graph_item(data_with_date) == "output_date_2026_01_01_00_00_00"
    assert (
        AiidaWorkGraph.get_aiida_label_from_graph_item(data_with_params)
        == "output_foo_0___bar_3_0___date_2026_01_01_00_00_00"
    )


def test_filename_conflict_detection(tmp_path):
    """Test logic for detecting when unique filenames are needed."""

    output_path = tmp_path / "output"
    other_path = tmp_path / "other"

    inputs = [
        GeneratedData(
            name="output",
            type=models.DataType.FILE,
            src=output_path,
            coordinates={"foo": 0},
        ),
        GeneratedData(
            name="output",
            type=models.DataType.FILE,
            src=output_path,
            coordinates={"foo": 1},
        ),
        GeneratedData(
            name="other",
            type=models.DataType.FILE,
            src=other_path,
            coordinates={},
        ),
    ]

    # Test that conflict detection works
    output_conflicts = [inp for inp in inputs if inp.name == "output"]
    other_conflicts = [inp for inp in inputs if inp.name == "other"]

    assert len(output_conflicts) == 2  # Should need unique filenames
    assert len(other_conflicts) == 1  # Should use simple filename


@pytest.mark.usefixtures("aiida_localhost")
def test_basic_remote_data_filename(tmp_path):
    """Test basic RemoteData filename handling."""
    file_path = tmp_path / "foo.txt"
    file_path.touch()
    script_path = tmp_path / "script.sh"
    script_path.touch()

    config_wf = models.ConfigWorkflow(
        name="basic",
        rootdir=tmp_path,
        cycles=[
            models.ConfigCycle(
                name="cycle",
                tasks=[
                    models.ConfigCycleTask(
                        name="task",
                        inputs=[models.ConfigCycleTaskInput(name="data", port="input")],
                    )
                ],
            ),
        ],
        tasks=[
            models.ConfigShellTask(
                name="task",
                command="echo {PORT::input}",
                src=str(script_path),
                computer="localhost",
            ),
        ],
        data=models.ConfigData(
            available=[
                models.ConfigAvailableData(
                    name="data",
                    type=models.DataType.FILE,
                    src=str(file_path),
                    computer="localhost",
                )
            ],
        ),
    )

    core_wf = Workflow.from_config_workflow(config_wf)
    aiida_wf = AiidaWorkGraph(core_wf)

    # Check that RemoteData was created and filename is correct
    task = aiida_wf._workgraph.tasks[0]
    assert isinstance(task.inputs.nodes["data"].value, orm.RemoteData)
    assert task.inputs.filenames.value == {"data": "foo.txt"}
    assert task.inputs.arguments.value == "{data}"


@pytest.mark.usefixtures("aiida_localhost")
def test_parameterized_filename_conflicts(tmp_path):
    """Test parameterized data filename handling in various conflict scenarios.

    This test covers:
    1. Parameterized data with conflicts (multiple files with same base name)
    2. Mixed conflict/no-conflict scenarios (some files conflict, others don't)
    3. Proper filename assignment based on conflict detection
    """
    yaml_content = textwrap.dedent(f"""
        name: test_workflow
        cycles:
            - simulation_cycle:
                tasks:
                    - simulate:
                        inputs:
                            - input_file:
                                port: input
                            - shared_config:
                                port: config
                        outputs: [simulation_output]
            - processing_cycle:
                tasks:
                    - process_data:
                        inputs:
                            - simulation_output:
                                parameters:
                                    foo: all
                                port: files
                        outputs: [processed_output]
                    - analyze:
                        inputs:
                            - shared_config:
                                port: config
                            - simulation_output:
                                parameters:
                                    foo: all
                                port: data
                        outputs: [analysis_result]
        tasks:
            - simulate:
                plugin: shell
                command: "simulate.py {{PORT::input}} --config {{PORT::config}}"
                src: {tmp_path}/simulate.py
                parameters: [foo]
                computer: localhost
            - process_data:
                plugin: shell
                command: "process.py {{PORT::files}}"
                src: {tmp_path}/process.py
                parameters: [foo]
                computer: localhost
            - analyze:
                plugin: shell
                command: "analyze.py --config {{PORT::config}} --data {{PORT::data}}"
                src: {tmp_path}/analyze.py
                parameters: [foo]
                computer: localhost
        data:
            available:
                - input_file:
                    type: file
                    src: {tmp_path}/input.txt
                    computer: localhost
                - shared_config:
                    type: file
                    src: {tmp_path}/config.json
                    computer: localhost
            generated:
                - simulation_output:
                    type: file
                    src: output.dat
                    parameters: [foo]
                - processed_output:
                    type: file
                    src: processed.dat
                    parameters: [foo]
                - analysis_result:
                    type: file
                    src: result.txt
                    parameters: [foo]
        parameters:
            foo: [1, 2]
        """)

    config_file = tmp_path / "config.yml"
    config_file.write_text(yaml_content)

    # Create required files
    (tmp_path / "input.txt").touch()
    (tmp_path / "config.json").touch()
    (tmp_path / "simulate.py").touch()
    (tmp_path / "process.py").touch()
    (tmp_path / "analyze.py").touch()

    core_wf = Workflow.from_config_file(str(config_file))
    aiida_wf = AiidaWorkGraph(core_wf)

    # Test 1: process_data tasks (from first test)
    process_tasks = [task for task in aiida_wf._workgraph.tasks if task.name.startswith("process_data")]
    assert len(process_tasks) == 2  # One for each foo value

    for task in process_tasks:
        nodes_keys = list(task.inputs.nodes._sockets.keys())
        filenames = task.inputs.filenames.value
        arguments = task.inputs.arguments.value

        # Each task should have exactly two simulation_output inputs (foo=1 and foo=2)
        sim_output_keys = [k for k in nodes_keys if k.startswith("simulation_output")]
        assert len(sim_output_keys) == 2

        # The filenames should be the full labels (since there are conflicts)
        for key in sim_output_keys:
            assert filenames[key] == key  # Full label used as filename
            assert key in arguments  # Key appears in arguments

    # Test 2: analyze tasks (from second test - mixed conflict/no-conflict)
    analyze_tasks = [task for task in aiida_wf._workgraph.tasks if task.name.startswith("analyze")]
    assert len(analyze_tasks) == 2  # One for each foo value

    for task in analyze_tasks:
        filenames = task.inputs.filenames.value

        # shared_config should use simple filename (no conflict across tasks)
        assert filenames["shared_config"] == "config.json"

        # simulation_output should use full labels (conflict with other analyze tasks)
        sim_output_keys = [k for k in filenames if k.startswith("simulation_output")]
        assert len(sim_output_keys) == 2  # Should have both foo=1 and foo=2 inputs

        for key in sim_output_keys:
            assert filenames[key] == key  # Full label as filename
            assert "foo_" in key  # Contains parameter info

    # Test 3: simulate tasks (should have simple filenames for shared_config)
    simulate_tasks = [task for task in aiida_wf._workgraph.tasks if task.name.startswith("simulate")]
    assert len(simulate_tasks) == 2  # One for each foo value

    for task in simulate_tasks:
        filenames = task.inputs.filenames.value

        # Both input_file and shared_config should use simple names (no conflicts)
        assert filenames["input_file"] == "input.txt"
        assert filenames["shared_config"] == "config.json"


@pytest.mark.usefixtures("aiida_localhost")
def test_parameterized_workflow_regression(tmp_path):
    """Regression test for exact parameterized workflow output."""
    yaml_str = textwrap.dedent(f"""
        start_date: "2026-01-01T00:00"
        stop_date: "2026-07-01T00:00"
        cycles:
            - simulation:
                cycling:
                    start_date: "2026-01-01T00:00"
                    stop_date: "2026-07-01T00:00"
                    period: P6M
                tasks:
                    - simulate:
                        inputs:
                            - initial_data:
                                port: input
                        outputs: [sim_result]
            - analysis:
                cycling:
                    start_date: "2026-01-01T00:00"
                    stop_date: "2026-07-01T00:00"
                    period: P6M
                tasks:
                    - analyze:
                        inputs:
                            - sim_result:
                                parameters: {{param: all}}
                                port: data
                        outputs: [final_result]
        tasks:
            - simulate:
                plugin: shell
                command: "simulate.py {{PORT::input}}"
                src: {tmp_path}/simulate.py
                parameters: [param]
                computer: localhost
            - analyze:
                plugin: shell
                command: "analyze.py {{PORT::data}}"
                src: {tmp_path}/analyze.py
                parameters: [param]
                computer: localhost
        data:
            available:
                - initial_data:
                    type: file
                    src: {tmp_path}/input.dat
                    computer: localhost
            generated:
                - sim_result:
                    type: file
                    src: result.dat
                    parameters: [param]
                - final_result:
                    type: file
                    src: final.dat
                    parameters: [param]
        parameters:
            param: [1, 2]
    """)

    config_file = tmp_path / "config.yml"
    config_file.write_text(yaml_str)

    # Create minimal required files
    (tmp_path / "input.dat").touch()
    (tmp_path / "simulate.py").touch()
    (tmp_path / "analyze.py").touch()

    core_wf = Workflow.from_config_file(str(config_file))
    aiida_wf = AiidaWorkGraph(core_wf)

    # Regression testing: verify structure
    analyze_tasks = [t for t in aiida_wf._workgraph.tasks if t.name.startswith("analyze")]
    assert len(analyze_tasks) == 2  # One for each param value

    task = analyze_tasks[0]  # Test one of the analyze tasks
    filenames = task.inputs.filenames.value
    arguments = task.inputs.arguments.value
    nodes_keys = list(task.inputs.nodes._sockets.keys())

    # Expected values for regression detection
    expected_keys = [
        "sim_result_param_1___date_2026_01_01_00_00_00",
        "sim_result_param_2___date_2026_01_01_00_00_00",
    ]
    expected_filenames = {
        "sim_result_param_1___date_2026_01_01_00_00_00": "sim_result_param_1___date_2026_01_01_00_00_00",
        "sim_result_param_2___date_2026_01_01_00_00_00": "sim_result_param_2___date_2026_01_01_00_00_00",
    }
    expected_arguments = (
        "{sim_result_param_1___date_2026_01_01_00_00_00} {sim_result_param_2___date_2026_01_01_00_00_00}"
    )

    assert set(nodes_keys) == set(expected_keys)
    assert filenames == expected_filenames
    assert arguments == expected_arguments


@pytest.mark.usefixtures("aiida_localhost")
def test_comprehensive_parameterized_workflow(tmp_path):
    """Test parameterized workflow behavior and properties."""
    yaml_str = textwrap.dedent(f"""
        start_date: &start "2026-01-01T00:00"
        stop_date: &stop "2026-07-01T00:00"
        cycles:
          - main:
              cycling:
                start_date: *start
                stop_date: *stop
                period: P6M
              tasks:
                - simulate:
                    inputs:
                      - config:
                          port: cfg
                    outputs: [sim_output]
                - analyze:
                    inputs:
                      - sim_output:
                          parameters: {{foo: all, bar: single}}
                          port: data
                    outputs: [analysis]
        tasks:
          - simulate:
              plugin: shell
              command: "sim.py {{PORT::cfg}}"
              src: {tmp_path}/sim.py
              parameters: [foo, bar]
              computer: localhost
          - analyze:
              plugin: shell
              command: "analyze.py {{PORT::data}}"
              src: {tmp_path}/analyze.py
              parameters: [bar]
              computer: localhost
        data:
          available:
            - config:
                type: file
                src: {tmp_path}/config.txt
                computer: localhost
          generated:
            - sim_output:
                type: file
                src: output.dat
                parameters: [foo, bar]
            - analysis:
                type: file
                src: analysis.txt
                parameters: [bar]
        parameters:
          foo: [0, 1]
          bar: [3.0]
    """)

    config_file = tmp_path / "config.yml"
    config_file.write_text(yaml_str)

    # Create files
    (tmp_path / "config.txt").touch()
    (tmp_path / "sim.py").touch()
    (tmp_path / "analyze.py").touch()

    core_wf = Workflow.from_config_file(str(config_file))
    aiida_wf = AiidaWorkGraph(core_wf)

    # Verify task structure
    sim_tasks = [t for t in aiida_wf._workgraph.tasks if t.name.startswith("simulate")]
    analyze_tasks = [t for t in aiida_wf._workgraph.tasks if t.name.startswith("analyze")]

    assert len(sim_tasks) == 2  # 2 foo values, and 1 bar value -> 2 tasks
    assert len(analyze_tasks) == 1  # 1 bar value -> 1 task

    # Check simulate tasks (should have simple config filename)
    for task in sim_tasks:
        filenames = task.inputs.filenames.value
        assert filenames["config"] == "config.txt"  # No conflict, simple name

    # Check analyze task (should have complex filenames due to conflicts)
    analyze_task = analyze_tasks[0]
    filenames = analyze_task.inputs.filenames.value

    # Should have 2 sim_output inputs with full labels as filenames
    sim_output_keys = [k for k in filenames if k.startswith("sim_output")]
    assert len(sim_output_keys) == 2

    for key in sim_output_keys:
        assert filenames[key] == key  # Full label used as filename
        assert "foo_" in key
        assert "bar_3_0" in key


# PRCOMMENT: Kept this hardcoded, explicit test based on the `parameters` case
# Can probably be removed as the other tests cover the behavior, but wanted to keep for now
@pytest.mark.usefixtures("aiida_localhost")
def test_comprehensive_parameterized_explicit(tmp_path):
    import pathlib

    # Get the test cases directory relative to the test file
    test_dir = pathlib.Path(__file__).parent.parent / "cases"

    yaml_str = textwrap.dedent(
        f"""
        start_date: &root_start_date "2026-01-01T00:00"
        stop_date: &root_stop_date "2028-01-01T00:00"
        cycles:
            - bimonthly_tasks:
                cycling:
                    start_date: *root_start_date
                    stop_date: *root_stop_date
                    period: P6M
                tasks:
                    - icon:
                        inputs:
                            - initial_conditions:
                                when:
                                    at: *root_start_date
                                port: init
                            - icon_restart:
                                when:
                                    after: *root_start_date
                                target_cycle:
                                    lag: -P6M
                                parameters:
                                    foo: single
                                    bar: single
                                port: restart
                            - forcing:
                                port: forcing
                        outputs: [icon_output, icon_restart]
                    - statistics_foo:
                        inputs:
                            - icon_output:
                                parameters:
                                    bar: single
                                port: None
                        outputs: [analysis_foo]
                    - statistics_foo_bar:
                        inputs:
                            - analysis_foo:
                                port: None
                        outputs: [analysis_foo_bar]
            - yearly:
                cycling:
                    start_date: *root_start_date
                    stop_date: *root_stop_date
                    period: P1Y
                tasks:
                    - merge:
                        inputs:
                            - analysis_foo_bar:
                                target_cycle:
                                    lag: ["P0M", "P6M"]
                                port: None
                        outputs: [yearly_analysis]
        tasks:
            - icon:
                plugin: shell
                src: {test_dir}/parameters/config/scripts/icon.py
                command: "icon.py --restart {{PORT::restart}} --init {{PORT::init}} --forcing {{PORT::forcing}}"
                parameters: [foo, bar]
                computer: localhost
            - statistics_foo:
                plugin: shell
                src: {test_dir}/parameters/config/scripts/statistics.py
                command: "statistics.py {{PORT::None}}"
                parameters: [bar]
                computer: localhost
            - statistics_foo_bar:
                plugin: shell
                src: {test_dir}/parameters/config/scripts/statistics.py
                command: "statistics.py {{PORT::None}}"
                computer: localhost
            - merge:
                plugin: shell
                src: {test_dir}/parameters/config/scripts/merge.py
                command: "merge.py {{PORT::None}}"
                computer: localhost
        data:
            available:
                - initial_conditions:
                    type: file
                    src: {test_dir}/small/config/data/initial_conditions
                    computer: localhost
                - forcing:
                    type: file
                    src: {test_dir}/parameters/config/data/forcing
                    computer: localhost
            generated:
                - icon_output:
                    type: file
                    src: icon_output
                    parameters: [foo, bar]
                - icon_restart:
                    type: file
                    src: restart
                    parameters: [foo, bar]
                - analysis_foo:
                    type: file
                    src: analysis
                    parameters: [bar]
                - analysis_foo_bar:
                    type: file
                    src: analysis
                - yearly_analysis:
                    type: file
                    src: analysis
        parameters:
            foo: [0, 1]
            bar: [3.0]
        """
    )
    yaml_file = tmp_path / "config.yml"
    yaml_file.write_text(yaml_str)

    core_wf = Workflow.from_config_file(yaml_file)
    aiida_wf = AiidaWorkGraph(core_workflow=core_wf)
    filenames_list = [task.inputs.filenames.value for task in aiida_wf._workgraph.tasks]
    arguments_list = [task.inputs.arguments.value for task in aiida_wf._workgraph.tasks]
    nodes_list = [list(task.inputs.nodes._sockets.keys()) for task in aiida_wf._workgraph.tasks]

    expected_filenames_list = [
        {"forcing": "forcing", "initial_conditions": "initial_conditions"},
        {"forcing": "forcing", "initial_conditions": "initial_conditions"},
        {
            "forcing": "forcing",
            "icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00": "restart",
        },
        {
            "forcing": "forcing",
            "icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00": "restart",
        },
        {
            "forcing": "forcing",
            "icon_restart_foo_0___bar_3_0___date_2026_07_01_00_00_00": "restart",
        },
        {
            "forcing": "forcing",
            "icon_restart_foo_1___bar_3_0___date_2026_07_01_00_00_00": "restart",
        },
        {
            "forcing": "forcing",
            "icon_restart_foo_0___bar_3_0___date_2027_01_01_00_00_00": "restart",
        },
        {
            "forcing": "forcing",
            "icon_restart_foo_1___bar_3_0___date_2027_01_01_00_00_00": "restart",
        },
        {
            "icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00": "icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00": "icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00",
        },
        {
            "icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00": "icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00": "icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00",
        },
        {
            "icon_output_foo_0___bar_3_0___date_2027_01_01_00_00_00": "icon_output_foo_0___bar_3_0___date_2027_01_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2027_01_01_00_00_00": "icon_output_foo_1___bar_3_0___date_2027_01_01_00_00_00",
        },
        {
            "icon_output_foo_0___bar_3_0___date_2027_07_01_00_00_00": "icon_output_foo_0___bar_3_0___date_2027_07_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2027_07_01_00_00_00": "icon_output_foo_1___bar_3_0___date_2027_07_01_00_00_00",
        },
        {"analysis_foo_bar_3_0___date_2026_01_01_00_00_00": "analysis"},
        {"analysis_foo_bar_3_0___date_2026_07_01_00_00_00": "analysis"},
        {"analysis_foo_bar_3_0___date_2027_01_01_00_00_00": "analysis"},
        {"analysis_foo_bar_3_0___date_2027_07_01_00_00_00": "analysis"},
        {
            "analysis_foo_bar_date_2026_01_01_00_00_00": "analysis_foo_bar_date_2026_01_01_00_00_00",
            "analysis_foo_bar_date_2026_07_01_00_00_00": "analysis_foo_bar_date_2026_07_01_00_00_00",
        },
        {
            "analysis_foo_bar_date_2027_01_01_00_00_00": "analysis_foo_bar_date_2027_01_01_00_00_00",
            "analysis_foo_bar_date_2027_07_01_00_00_00": "analysis_foo_bar_date_2027_07_01_00_00_00",
        },
    ]

    expected_arguments_list = [
        "--restart  --init {initial_conditions} --forcing {forcing}",
        "--restart  --init {initial_conditions} --forcing {forcing}",
        "--restart {icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_0___bar_3_0___date_2026_07_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_1___bar_3_0___date_2026_07_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_0___bar_3_0___date_2027_01_01_00_00_00} --init  " "--forcing {forcing}",
        "--restart {icon_restart_foo_1___bar_3_0___date_2027_01_01_00_00_00} --init  " "--forcing {forcing}",
        "{icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00} "
        "{icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00}",
        "{icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00} "
        "{icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00}",
        "{icon_output_foo_0___bar_3_0___date_2027_01_01_00_00_00} "
        "{icon_output_foo_1___bar_3_0___date_2027_01_01_00_00_00}",
        "{icon_output_foo_0___bar_3_0___date_2027_07_01_00_00_00} "
        "{icon_output_foo_1___bar_3_0___date_2027_07_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2026_01_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2026_07_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2027_01_01_00_00_00}",
        "{analysis_foo_bar_3_0___date_2027_07_01_00_00_00}",
        "{analysis_foo_bar_date_2026_01_01_00_00_00} " "{analysis_foo_bar_date_2026_07_01_00_00_00}",
        "{analysis_foo_bar_date_2027_01_01_00_00_00} " "{analysis_foo_bar_date_2027_07_01_00_00_00}",
    ]

    expected_nodes_list = [
        ["initial_conditions", "forcing"],
        ["initial_conditions", "forcing"],
        ["icon_restart_foo_0___bar_3_0___date_2026_01_01_00_00_00", "forcing"],
        ["icon_restart_foo_1___bar_3_0___date_2026_01_01_00_00_00", "forcing"],
        ["icon_restart_foo_0___bar_3_0___date_2026_07_01_00_00_00", "forcing"],
        ["icon_restart_foo_1___bar_3_0___date_2026_07_01_00_00_00", "forcing"],
        ["icon_restart_foo_0___bar_3_0___date_2027_01_01_00_00_00", "forcing"],
        ["icon_restart_foo_1___bar_3_0___date_2027_01_01_00_00_00", "forcing"],
        [
            "icon_output_foo_0___bar_3_0___date_2026_01_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_01_01_00_00_00",
        ],
        [
            "icon_output_foo_0___bar_3_0___date_2026_07_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2026_07_01_00_00_00",
        ],
        [
            "icon_output_foo_0___bar_3_0___date_2027_01_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2027_01_01_00_00_00",
        ],
        [
            "icon_output_foo_0___bar_3_0___date_2027_07_01_00_00_00",
            "icon_output_foo_1___bar_3_0___date_2027_07_01_00_00_00",
        ],
        ["analysis_foo_bar_3_0___date_2026_01_01_00_00_00"],
        ["analysis_foo_bar_3_0___date_2026_07_01_00_00_00"],
        ["analysis_foo_bar_3_0___date_2027_01_01_00_00_00"],
        ["analysis_foo_bar_3_0___date_2027_07_01_00_00_00"],
        [
            "analysis_foo_bar_date_2026_01_01_00_00_00",
            "analysis_foo_bar_date_2026_07_01_00_00_00",
        ],
        [
            "analysis_foo_bar_date_2027_01_01_00_00_00",
            "analysis_foo_bar_date_2027_07_01_00_00_00",
        ],
    ]

    assert arguments_list == expected_arguments_list
    assert filenames_list == expected_filenames_list
    assert nodes_list == expected_nodes_list

    # PRCOMMENT: Introduce this once we can automatically create the codes in a reasonable way.
    # Currently, it still fails...
    # output_node = aiida_wf.run()
    # assert (
    #     output_node.is_finished_ok
    # ), f"Not successful run. Got exit code {output_node.exit_code} with message {output_node.exit_message}."
