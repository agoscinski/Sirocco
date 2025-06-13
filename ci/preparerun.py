# DON

import shutil
from pathlib import Path
from sirocco.core import Workflow
from sirocco.workgraph import AiidaWorkGraph

test_case = Path("../tests/cases/small-icon/").absolute()
workdir = Path("../workdir").absolute()
if workdir.exists():
    workdir.rmdir()
workdir.mkdir()
shutil.copytree(test_case, workdir)
yml_path = workdir / "config/config.yml"
yml_path.read_text().replace("/TESTS_ROOTDIR", str(workdir))
yml_path.read_text().replace("/REMOTE_DATADIR", str(workdir))
# TODO maybe need to copy remote datadir

core_workflow = Workflow.from_config_file(workdir / "config.yml")
aiida_workflow = AiidaWorkGraph(core_workflow)
output_node = aiida_workflow.run()



#config[key].write_text(config[key].read_text().replace("/TESTS_ROOTDIR", str(tmp_path)))
#"/capstor/store/cscs/userlab/cwd01/ricoh"

# real test case
#/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_cases/exclaim_ape_R02B04
