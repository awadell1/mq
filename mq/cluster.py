from dataclasses import dataclass
import subprocess
from abc import abstractmethod
import jq
from os import getlogin
from shutil import which
from typing import Iterable, Optional


@dataclass
class Job:
    id: str
    name: str
    status: str
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
            "state: .job_state[0], "
            "queue: .partition, "
            "nodes: .job_resources.allocated_nodes[].nodename, "
            "host: .batch_host, "
            "stdout: .standard_output, "
            "}"
        ).input_text(out.stdout)
        for job in jobs:
            yield Job(
                id=job["id"],
                name=job["name"],
                status=job["state"],
                nodes=job["nodes"],
                queue=job["queue"],
                host=job["host"],
                stdout=job["stdout"],
            )
