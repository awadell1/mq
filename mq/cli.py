import typer
import json
import rich
from rich.console import Console
from rich.table import Table
from .cluster import Cluster
from sys import stdout
from typing import Optional
import shutil

cli = typer.Typer(rich_markup_mode="markdown")


@cli.command()
def raw():
    """User job information in JSON"""
    manager = Cluster.get_cluster_manager()
    jobs = manager.get_jobs()
    json.dump(list(jobs), stdout, default=lambda o: o.__dict__)


@cli.command()
def cat(job_id: Optional[str] = typer.Argument(default="last")):
    """cat a jobs output"""
    manager = Cluster.get_cluster_manager()
    jobs = manager.get_jobs()
    if job_id == "last":
        *_, job = jobs
        jobs = [job]
    elif job_id == "all":
        jobs = jobs
    else:
        jobs = filter(lambda job: str(job.id) == job_id, jobs)

    for job in jobs:
        if job_output := job.stdout:
            with open(job_output, "r") as fid:
                shutil.copyfileobj(fid, stdout)


@cli.callback(invoke_without_command=True)
def status(ctx: typer.Context):
    """A nicer PBS qstat for the current user"""

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
    for job in manager.get_jobs():
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
