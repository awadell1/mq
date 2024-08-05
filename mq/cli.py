import json
import os
import shutil
import time
from collections import deque
from sys import stdout
from typing import Optional

import rich
import typer
from rich.live import Live
from rich.panel import Panel
from rich.console import Console
from rich.table import Table

from .cluster import Cluster, Status, ACTIVE_JOB_STATES

cli = typer.Typer(rich_markup_mode="markdown")


@cli.command()
def raw(
    user: bool = typer.Option(
        True, help="Only return job information for the current user"
    )
):
    """JSON formatted Job information

    > Warning: Exact content may change without notice
    """
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
    """Prints a job's standard output"""
    manager = Cluster.get_cluster_manager()
    jobs = filter_jobs(manager.get_jobs(), job_id)

    for job in jobs:
        if job_output := job.stdout:
            with open(job_output, "r") as fid:
                shutil.copyfileobj(fid, stdout)


@cli.command()
def tail(
    job_id: str = typer.Argument(default="all"),
    watch: bool = typer.Option(True),
    n: int = typer.Option(
        shutil.get_terminal_size().lines,
        "--lines",
        "-n",
        help="number of lines to display",
    ),
):
    """Prints the tail of a job's standard output

    By default will watch the output off all active (Pending, Running or Completing) jobs,
    but can be configured to watch a subset of jobs or only print the tail once.
    """
    manager = Cluster.get_cluster_manager()
    jobs = list(filter_jobs(manager.get_jobs(), job_id))
    console = Console(soft_wrap=False)
    grid = Table.grid()

    with Live(grid, transient=True) as live:
        done = False
        while not done:
            grid, done = create_tail_grid(console.size.height - 2, manager, jobs)
            live.update(
                grid,
                refresh=True,
            )
            time.sleep(5)


def create_tail_grid(lines, manager, jobs):
    grid = Table.grid()
    grid.title = f"{time.asctime()}"
    grid.highlight = True

    tail_states = [Status.RUNNING, Status.COMPLETING]

    # Count active jobs
    job_state = {}
    n_active = 0
    n_pending = 0
    keep_going = False
    for job in jobs:
        state = manager.job_status(job)
        job_state[job.id] = state
        n_active += 1 if state in tail_states else 0
        n_pending += 1 if state == Status.PENDING else 0
        keep_going = keep_going or state in ACTIVE_JOB_STATES
    lines_per_job = lines // max(n_active, 1)

    if n_pending > 0:
        grid.title += f" ({n_pending:d} pending)"

    hg = rich.highlighter.ReprHighlighter()

    for job in list(jobs):
        status = job_state[job.id]
        if status in tail_states:
            grid.add_row(
                Panel(
                    hg(
                        rich.text.Text(
                            _job_tail(manager, job, lines) or "",
                            overflow="ellipse",
                            no_wrap=True,
                        )
                    ),
                    title=f"{job.id} - {job.status} - {job.host}",
                    highlight=True,
                    height=lines_per_job,
                )
            )

    return grid, not keep_going


def _job_tail(manager, job, lines):
    if job_output := job.stdout:
        if os.path.isfile(job_output):
            with open(job_output, "r") as fid:
                job_tail = deque(fid, lines)

            return "".join(job_tail)


@cli.callback(invoke_without_command=True)
def status(ctx: typer.Context):
    """mq: A pretty queue for HPC schedulers

    Plus a bundle of UI/CLI tools for monitoring job progress/status
    """

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
