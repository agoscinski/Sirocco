from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, ClassVar, Self

import f90nml

from sirocco.core.graph_items import Task
from sirocco.parsing import yaml_data_models as models
from sirocco.parsing.cycling import DateCyclePoint


@dataclass(kw_only=True)
class ParsedNamelist(models.NamelistSpec):
    content: f90nml.Namelist

    @classmethod
    def from_config(cls, namelist: models.ConfigNamelist):
        content = f90nml.read(namelist.path)
        self = cls(path=namelist.path, specs=namelist.specs, content=content)
        self.update_with_specs(self.specs)
        return self

    @property
    def name(self) -> str:
        return self.path.name

    def update_with_specs(self, specs: dict[str, Any]):
        for section, params in specs.items():
            section_name, k = self.section_index(section)
            # Create section if non-existent
            if section_name not in self.content:
                # NOTE: f90nml will automatially create the corresponding nested f90nml.Namelist
                #       objects, no need to explicitly use the f90nml.Namelist class constructor
                self.content[section_name] = {} if k is None else [{}]
            # Update namelist with user input
            # NOTE: unlike FORTRAN convention, user index starts at 0 as in Python
            if k == len(self.content[section_name]) + 1:
                # Create additional section if required
                self.content[section_name][k] = f90nml.Namelist()
            nml_section = self.content[section_name] if k is None else self.content[section_name][k]
            nml_section.update(params)

    @staticmethod
    def section_index(section_name) -> tuple[str, int | None]:
        """Check for single vs multiple namelist section

        Check if the user specified a section name that ends with digits
        between brackets, for example:

        section_index("section[123]") -> ("section", 123)
        section_index("section123") -> ("section123", None)

        This is the convention chosen to indicate multiple
        sections with the same name, typically `output_nml` for multiple
        output streams."""
        multi_section_pattern = re.compile(r"(.*)\[([0-9]+)\]$")
        if m := multi_section_pattern.match(section_name):
            return m.group(1), int(m.group(2)) - 1
        return section_name, None

@dataclass(kw_only=True)
class IconTask(models.ConfigIconTaskSpecs, Task):
    _MASTER_NAMELIST_NAME: ClassVar[str] = "icon_master.namelist"
    _AIIDA_ICON_RESTART_PORT_NAME = "restart_file"
    namelists: list[ParsedNamelist]
 
    def __post_init__(self):
        super().__post_init__()
        for namelist in self.namelists:
            if namelist.path.is_relative_to("."): 
                namelist.path = self.config_rootdir / namelist.path
            if not namelist.path.exists():
                raise FileNotFoundError(f"Icon executable in source {namelist.path} does not exist.")
            if not namelist.path.is_file():
                raise ValueError(f"Icon executable in source {namelist.path} is not file.")

        namelists = []
        for namelist in self.namelists:
            namelists.append(ParsedNamelist.from_config(namelist))
        self.namelists = namelists

        index_master_namelist = None
        for i, namelist in enumerate(self.namelists):
            if namelist.name == self._MASTER_NAMELIST_NAME:
                index_master_namelist = i
        if index_master_namelist is None:
            raise ValueError(f"Failed to read master namelists. Could not find {self._MASTER_NAMELIST_NAME!r} in namelists {self.namelists}")
        self._index_master_namelist = index_master_namelist
        
        # TODO check with Matthieu if only way to specify model namelist, then make them ClassVar
        if (master_model_nml := self.master_namelist.content.get("master_model_nml", None)) is None:
            raise ValueError("No model filename specified in master namelist. Could not find section 'master_model_nml'")
        if (model_namelist_filename := master_model_nml.get("model_namelist_filename")) is None:
            raise ValueError("No model filename specified in master namelist. Could not find entry 'model_namelist_filename' under section 'master_model_nml'")
         
        index_model_namelist = None
        for i, namelist in enumerate(self.namelists):
            if namelist.name == model_namelist_filename:
                index_model_namelist = i
        if index_model_namelist is None:
            raise ValueError(f"Failed to read model namelist. Could not find {model_namelist_filename!r} in namelists {self.namelists}")
        self._index_model_namelist = index_model_namelist

    @property
    def master_namelist(self) -> ParsedNamelist:
        return self.namelists[self._index_master_namelist]

    @property
    def model_namelist(self) -> ParsedNamelist:
        return self.namelists[self._index_model_namelist]

    #def _init_core_namelists(self):
    #    """Read in or create namelists"""
    #    # PR COMMENT I think its better to create a new class that enforces this duality of files, and f90.namelist
    #    self.core_namelists = []
    #    self._core_master_namelist = None
    #    for cfg_nml in self.namelists:
    #        if cfg_nml.path.name == "icon_master.namelist":
    #            self._core_master_namelist = self.core_namelists
    #        if (nml_path := self.config_rootdir / cfg_nml.path).exists():
    #            self.core_namelists.append( f90nml.read(nml_path) )
    #        else: # PR COMMENT would remove this for simlification, This would need to be handled for example also in workgraph.py to create a correct file
    #            # If namelist does not exist, build it from the users given specs
    #            self.core_namelists.append( f90nml.Namelist() )
    #    if self._core_master_namelist is None:
    #        raise ValueError(f"Failed to read namelists: Could not find 'icon_master.namlist'. from namelists {self.namelists}")

    #def update_namelists_from_spe(self):
    #    """Update the core namelists from namelists provided by the user in the config yaml file."""
    #    for namelist in self.namelists:
    #        namelist._update_from_specs()

    #    # TODO: implement format for users to reference parameters and date in their specs
    #    for i in self.namelists):
    #        core_nml = self.core_namelists[i]
    #        if cfg_nml.specs is None:
    #            continue
    #        for section, params in cfg_nml.specs.items():
    #            section_name, k = self.section_index(section)
    #            # Create section if non-existent
    #            if section_name not in core_nml:
    #                # NOTE: f90nml will automatially create the corresponding nested f90nml.Namelist
    #                #       objects, no need to explicitly use the f90nml.Namelist class constructor
    #                core_nml[section_name] = {} if k is None else [{}]
    #            # Update namelist with user input
    #            # NOTE: unlike FORTRAN convention, user index starts at 0 as in Python
    #            if k == len(core_nml[section_name]) + 1:
    #                # Create additional section if required
    #                core_nml[section_name][k] = f90nml.Namelist()
    #            nml_section = core_nml[section_name] if k is None else core_nml[section_name][k]
    #            nml_section.update(params)


    @property
    def is_restart(self) -> bool:
        """Check if the restart file is present in the run.

        TODO: At the moment we use the data name to check if it is a restart file but eventually this should check the port."""
        # Because of parameterization, each input is parametrized so we only check the first none
        # TODO there is a bug here input_ is an sometimes empty list
        return self._AIIDA_ICON_RESTART_PORT_NAME in self.inputs

    def update_namelists_from_workflow(self):
        if not isinstance(self.cycle_point, DateCyclePoint):
            msg = "ICON task must have a DateCyclePoint"
            raise TypeError(msg)
        self.master_namelist.update_with_specs({
            "master_time_control_nml": {
                    "experimentStartDate": self.cycle_point.start_date.isoformat() + "Z",
                    "experimentStopDate": self.cycle_point.stop_date.isoformat() + "Z",  # TODO isn't the restart interval needed to be updated?
            },
            'master_nml': {
                "lrestart": self.is_restart
                "read_restart_namelists": self.is_restart # PR COMMENT not sure @matthieu
            }
        })

    #@staticmethod
    #def namelist_to_string(namelist: ParsedNamelist) -> str:
    #    import io
    #    with io.StringIO() as handler:
    #        namelist.content.write(handler)
    #    icon_task.inputs.master_namelist.value = aiida.orm.SinglefileData.from_string(content, "icon_master.namelist")
    #            self.namelists.write(stream)
    #    return stream.getvalue()
    #    breakpoint()
    #    #

    #    return 
    #    #if folder is not None:
    #    #    folder = Path(folder)
    #    #    folder.mkdir(parents=True, exist_ok=True)
    #    #for i, cfg_nml in enumerate(self.namelists):
    #    #    if folder is None:
    #    #        folder = (self.config_rootdir / cfg_nml.path).parent
    #    #    suffix = ("_".join([str(p) for p in self.coordinates.values()])).replace(" ", "_")
    #    #    self.core_namelists[i].write(folder / (cfg_nml.path.name + "_" + suffix), force=True)

    @classmethod
    def build_from_config(cls: type[Self], config: models.ConfigTask, **kwargs: Any) -> Self:
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
        # The following check is here for type checkers.
        # We don't want to narrow the type in the signature, as that would break liskov substitution.
        # We guarantee elsewhere this is called with the correct type at runtime
        if not isinstance(config, models.ConfigIconTask):
            raise TypeError
        config_kwargs["namelists"] = {
            nml.path.name: models.NamelistSpec(**nml.model_dump()) for nml in config.namelists
        }
        return cls(
            **kwargs,
            **config_kwargs,
        )
