from dataclasses import dataclass
import subprocess
from abc import abstractmethod
import jq
from os import getlogin
from shutil import which
from typing import Iterable, Optional, Union
from enum import StrEnum, auto


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
    nodes: Optional[list[str]]
    queue: str
    host: Optional[str]
    stdout: Optional[str]

    @property
    def node_count(self) -> Optional[int]:
        if self.nodes:
            len(self.nodes)


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

    def get_jobs(self, user=True) -> Iterable[Job]:
        out = subprocess.run(
            ["-f", "-F", "json"],
            executable=self.qstat,
            capture_output=True,
            check=True,
            encoding="utf-8",
        )

        JQ_USER_JOBS = jq.compile(
            f'.Jobs | [to_entries[] | {{jobid: .key}} + .value][] | select(.Job_Owner | startswith("{getlogin()}@"))'
        )
        return JQ_USER_JOBS.input_text(out.stdout).all()


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
