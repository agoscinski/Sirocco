# DON

import os
import shutil
from pathlib import Path

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


#config[key].write_text(config[key].read_text().replace("/TESTS_ROOTDIR", str(tmp_path)))
#"/capstor/store/cscs/userlab/cwd01/ricoh"

# real test case
#/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_cases/exclaim_ape_R02B04
