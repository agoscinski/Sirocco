import textwrap

import pytest
from pydantic import ValidationError

from sirocco.parsing import _yaml_data_models as models


@pytest.mark.parametrize("data_type", ["file", "dir"])
def test_base_data(data_type):
    testee = models.ConfigBaseData(name="name", type=data_type, src="foo.txt", format=None)

    assert testee.type == data_type


@pytest.mark.parametrize("data_type", [None, "invalid", 1.42])
def test_base_data_invalid_type(data_type):
    with pytest.raises(ValidationError):
        _ = models.ConfigBaseData(name="name", src="foo", format="nml")

    with pytest.raises(ValidationError):
        _ = models.ConfigBaseData(name="name", type=data_type, src="foo", format="nml")


@pytest.fixture
def minimal_config_path(tmp_path):
    minimal_config = textwrap.dedent(
        """
        cycles:
          - minimal:
              tasks:
                - a:
        tasks:
          - b:
              plugin: shell
        data:
          available:
            - c:
                type: "file"
                src: "c.txt"
          generated:
            - d:
                type: "dir"
                src: "d"
        """
    )
    minimal = tmp_path / "minimal.yml"
    minimal.write_text(minimal_config)
    return minimal


def test_load_workflow_config(minimal_config_path):
    testee = models.ConfigWorkflow.from_config_file(str(minimal_config_path))
    assert testee.name == "minimal"
    assert testee.rootdir == minimal_config_path.parent
