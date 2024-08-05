import subprocess
from abc import abstractmethod
from dataclasses import dataclass
from enum import StrEnum, auto
from os import getlogin
from shutil import which
from typing import Iterable, Optional, Union

import jq


class Status(StrEnum):
    PENDING = auto()
    RUNNING = auto()
    COMPLETING = auto()
    COMPLETED = auto()
    FAILED = auto()
    OTHER = auto()

    def __dict__(self):
        print(self)
        return self.name

    def __call__(cls, value, **kwargs):
        return super()(value.lower(), **kwargs)


ACTIVE_JOB_STATES = [Status.PENDING, Status.RUNNING, Status.COMPLETING]


@dataclass
class Job:
    id: str
    name: str
    user: str
    status: Status
    queue: str
    nodes: Optional[list[str]] = None
    _host: Optional[str] = None
    stdout: Optional[str] = None

    @property
    def node_count(self) -> Optional[int]:
        if self.nodes:
            len(self.nodes)

    @property
    def host(self) -> Optional[str]:
        if self._host:
            return self._host
        if self.nodes:
            return self.nodes[0]
        return None

    @host.setter
    def host(self, host):
        self._host = host


class Cluster:
    def get_jobs(self, user=True) -> Iterable[Job]:
        pass

    @staticmethod
    @abstractmethod
    def detect_cluster() -> bool:
        """Return True if this cluster is present on the machine"""
        raise NotImplementedError()

    def job_status(self, jobid: Union[Job, str]) -> Status:
        id = jobid.id if isinstance(jobid, Job) else jobid
        if job := next(filter(lambda job: job.id == id, self.get_jobs()), None):
            return job.status
        raise RuntimeError("No such job", id)

    @staticmethod
    def norm_status(status: str) -> Status:
        try:
            return Status(status.lower())
        except ValueError:
            return Status.OTHER

    @staticmethod
    def get_cluster_manager() -> "Cluster":
        for cls in Cluster.__subclasses__():
            if cls.detect_cluster():
                return cls()
        raise RuntimeError("No supported cluster found")


class PBS(Cluster):
    def __init__(self):
        self.qstat = which("qstat")

    @staticmethod
    def detect_cluster():
        return which("qstat") is not None

    @staticmethod
    def norm_status(status: str) -> Status:
        if status == "R":
            return Status.RUNNING
        elif status == "Q":
            return Status.PENDING
        elif status == "X":
            return Status.COMPLETING
        elif status == "F":
            return Status.COMPLETED
        else:
            return Cluster.norm_status(status)

    def get_jobs(self, user=True) -> Iterable[Job]:
        out = subprocess.run(
            [self.qstat, "-f", "-F", "json"],
            capture_output=True,
            check=True,
            encoding="utf-8",
        )
        jobs: Iterable[dict[str, str]] = jq.compile(
            """.Jobs | to_entries[] | {
                id: .key,
                name: .value.Job_Name,
                user: .value.Job_Owner,
                state: .value.job_state,
                queue: .value.queue,
                nodes: .value.exec_host?,
                stdout: .value.Output_Path?,
            }"""
        ).input_text(out.stdout)
        for job in jobs:
            if user and job["user"] != getlogin():
                continue
            yield Job(
                id=job["id"].partition(".")[0],
                name=job["name"],
                user=job["user"].partition("@")[0],
                status=self.norm_status(job["state"]),
                queue=job["queue"],
                nodes=(
                    [n.partition("/")[0] for n in job["nodes"].split("+")]
                    if job["nodes"]
                    else None
                ),
                stdout=(
                    (job["stdout"].partition(":")[2] or job["stdout"])
                    if job["stdout"]
                    else None
                ),
            )


class Slurm(Cluster):
    def __init__(self):
        self.squeue = which("squeue")

    @staticmethod
    def detect_cluster() -> bool:
        return which("squeue") is not None

    def get_jobs(self, user=True) -> Iterable[Job]:
        out = subprocess.run(
            ["--all", f"--user={getlogin()}", "--json"],
            executable=self.squeue,
            capture_output=True,
            check=True,
            encoding="utf-8",
        )
        jobs = jq.compile(
            ".jobs[] | { "
            "id: .job_id, "
            "name: .name, "
            "user: .user_name, "
            "state: .job_state[0]?, "
            "queue: .partition, "
            "nodes: ( .job_resources.allocated_nodes | .[0]?.nodename ) , "
            "host: .batch_host, "
            "stdout: .standard_output, "
            "}"
        ).input_text(out.stdout)
        for job in jobs:
            yield Job(
                id=job["id"],
                name=job["name"],
                user=job["user"],
                status=self.norm_status(job["state"]),
                nodes=job["nodes"],
                queue=job["queue"],
                host=job["host"],
                stdout=job["stdout"],
            )
