# DON

import shutil
from pathlib import Path
from aiida.orm import load_computer

from sirocco.core import Workflow
from sirocco.workgraph import AiidaWorkGraph


# put the download things to test utils
import requests
class DownloadError(RuntimeError):
    def __init__(self, url: str, response: requests.Response):
        super().__init__(f"Failed downloading file {url} , exited with response {response}")


def download_file(url: str, file_path: Path):
    response = requests.get(url)
    if not response.ok:
        raise DownloadError(url, response)

    file_path.write_bytes(response.content)


def download_icon_grid(dest_dir: Path, filename: str):
    url = "https://github.com/agoscinski/icon-testfiles/raw/refs/heads/main/icon_grid_0013_R02B04_R.nc"
    if not dest_dir.exists():
        raise ValueError(f"Destination directory {dest_dir} does not exist.")
    if not dest_dir.is_dir():
        raise ValueError(f"Destination directory {dest_dir} is not a directory.")

    icon_grid_path = dest_dir / filename
    download_file(url, icon_grid_path)

    return icon_grid_path

# must be run from repo rootdir
test_case = Path("tests/cases/small-icon/config").absolute()
workdir = Path("workdir").absolute()
if workdir.exists():
    workdir.rmdir()
(workdir / "tests/cases/small-icon/").mkdir(parents=True)
shutil.copytree(test_case, workdir / "tests/cases/small-icon/config")
yml_path = workdir / "tests/cases/small-icon/config/config.yml"

REMOTE_TESTSDIR = "/capstor/scratch/cscs/ricoh/sirocco-tests/"
yml_path.read_text().replace("/TESTS_ROOTDIR", REMOTE_TESTSDIR)
yml_path.read_text().replace("/REMOTE_DATADIR", REMOTE_TESTSDIR)

download_icon_grid(workdir / "tests/cases/small-icon/config/ICON", "icon_grid_simple.nc")


computer = load_computer('remote')
transport = computer.get_transport()
with transport:
    transport.rmtree(REMOTE_TESTSDIR)
    transport.mkdir(REMOTE_TESTSDIR)
    transport.puttree(localpath=workdir, remotepath=REMOTE_TESTSDIR)
    print(transport.listdir(REMOTE_TESTSDIR))
#for 
#    aiida_workflow.data
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/icon_grid_simple.nc
#         computer: localhost_ssh
#     - ecrad_data:
#         type: file
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/ecrad_data
#         computer: localhost_ssh
#     - ECHAM6_CldOptProps:
#         type: file
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/ECHAM6_CldOptProps.nc
#         computer: localhost_ssh
#     - rrtmg_sw:
#         type: file
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/rrtmg_sw.nc
#         computer: localhost_ssh
#     - dmin_wetgrowth_lookup:
#         type: file
#         src: /DATA_REMOTEDIR/tests/cases/small-icon/config/ICON/dmin_wetgrowth_lookup.nc
#
core_workflow = Workflow.from_config_file(str(workdir / "config.yml"))
aiida_workflow = AiidaWorkGraph(core_workflow)
output_node = aiida_workflow.run()

#config[key].write_text(config[key].read_text().replace("/TESTS_ROOTDIR", str(tmp_path)))
#"/capstor/store/cscs/userlab/cwd01/ricoh"

# real test case
#/capstor/store/cscs/userlab/cwd01/leclairm/Sirocco_test_cases/exclaim_ape_R02B04
