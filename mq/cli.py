import json
import os
import shutil
import time
from collections import deque
from sys import stdout
from typing import Optional

import rich
import typer
from rich.console import Console
from rich.table import Table

from .cluster import Cluster, Status, ACTIVE_JOB_STATES

cli = typer.Typer(rich_markup_mode="markdown")


@cli.command()
def raw():
    """User job information in JSON"""
    manager = Cluster.get_cluster_manager()
    jobs = manager.get_jobs()
    json.dump(list(jobs), stdout, default=lambda o: o.__dict__)


def filter_jobs(jobs, job_id):
    if job_id == "last":
        *_, job = jobs
        return [job]
    elif job_id == "all":
        return jobs
    else:
        return filter(lambda job: str(job.id) == job_id, jobs)


@cli.command()
def cat(job_id: Optional[str] = typer.Argument(default="last")):
    """cat a jobs output"""
    manager = Cluster.get_cluster_manager()
    jobs = filter_jobs(manager.get_jobs(), job_id)

    for job in jobs:
        if job_output := job.stdout:
            with open(job_output, "r") as fid:
                shutil.copyfileobj(fid, stdout)


@cli.command()
def tail(
    job_id: str = typer.Argument(default="last"),
    watch: bool = typer.Option(True),
    n: int = typer.Option(20, "--lines", "-n", help="number of lines to display"),
):
    """Display the last `n` lines of a job's stdout"""
    manager = Cluster.get_cluster_manager()
    jobs = filter_jobs(manager.get_jobs(), job_id)
    job = jobs[0]

    while (state := manager.job_status(job)) in ACTIVE_JOB_STATES:
        if watch:
            os.system("cls" if os.name == "nt" else "clear")
            print(f"{time.asctime()} - {job.id} ({state})")

        for job in jobs:
            if job_output := job.stdout:
                if os.path.isfile(job_output):
                    with open(job_output, "r") as fid:
                        job_tail = deque(fid, n)
                    stdout.write("".join(job_tail))

        if not watch:
            break
        time.sleep(1)


@cli.callback(invoke_without_command=True)
def status(ctx: typer.Context):
    """A nicer qstat/squeue for the current user"""

    # Bail if actually running a sub-command
    if ctx.invoked_subcommand is not None:
        return

    # Setup table
    console = Console()
    table = Table(header_style="bold", pad_edge=False, box=rich.box.SIMPLE_HEAD)
    table.add_column("id", max_width=10)
    table.add_column("name", max_width=10, overflow="ellipse")
    table.add_column("st", min_width=3, justify="center")
    table.add_column("N", min_width=2, max_width=8, justify="center")
    table.add_column("queue", min_width=6, max_width=20)
    table.add_column("host", width=16)

    manager = Cluster.get_cluster_manager()
    for job in filter(
        lambda job: job.status in ACTIVE_JOB_STATES,
        manager.get_jobs(),
    ):
        table.add_row(
            f"{job.id}",
            job.name,
            job.status,
            f"{job.node_count}" if job.node_count else "-",
            job.queue,
            job.host,
        )

    console.print(table)


if __name__ == "__main__":
    cli()
