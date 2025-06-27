from aiida.orm import Computer, load_computer

def aiida_computer_session(
  label: str | None = None,
  hostname="localhost",
  scheduler_type="core.direct",
  transport_type="core.local",
  minimum_job_poll_interval: int = 0,
  default_mpiprocs_per_machine: int = 1,
  configuration_kwargs: dict[t.Any, t.Any] | None = None,
) -> "Computer":
  import uuid

  from aiida.orm import Computer

  label = label or f"test-computer-{uuid.uuid4().hex}"

  computer = Computer.collection.get(
      label=label, hostname=hostname, scheduler_type=scheduler_type, transport_type=transport_type
  )

  if configuration_kwargs:
      computer.configure(**configuration_kwargs)

  return computer

computer = aiida_computer_session(label="remote", hostname="localhost", transport_type="core.ssh")

import os
computer.configure(
    key_filename=f"{os.environ['HOME']}/.ssh/id_rsa",
    key_policy="AutoAddPolicy",
    safe_interval=0.1,
)

