import typer
import json
import subprocess
import rich
from rich.console import Console
from rich.table import Table
import jq
from os import getlogin


cli = typer.Typer(rich_markup_mode="markdown")

# JQ to get the current users jobs
JQ_USER_JOBS = jq.compile(
    f'.Jobs | [to_entries[] | {{jobid: .key}} + .value][] | select(.Job_Owner | startswith("{getlogin()}@"))'
)


def user_jobs():
    out = subprocess.run(
        ["qstat", "-f", "-F", "json"],
        capture_output=True,
        check=True,
        encoding="utf-8",
    )
    # print(out.stdout)
    return JQ_USER_JOBS.input_text(out.stdout).all()


@cli.command()
def raw():
    """User job information in JSON"""
    print(json.dumps(user_jobs()))


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
    table.add_column("st", width=3, justify="center")
    table.add_column("N", min_width=2, max_width=8, justify="center")
    table.add_column("queue", width=6)
    table.add_column("host", width=16)

    for job in user_jobs():
        table.add_row(
            job["jobid"].split(".")[0],
            job["Job_Name"],
            job["job_state"],
            "%d" % job["Resource_List"]["nodect"],
            job["queue"],
            job.get("exec_host", "").split("/")[0] or None,
        )

    console.print(table)


if __name__ == "__main__":
    cli()
