try:
    from cli import show_help, show_add_user
    from cli.gen_environment import gen_environment
except ModuleNotFoundError:
    from .cli import show_help, show_add_user
    from .cli.gen_environment import gen_environment
import click
import pytest
import os
from pathlib import Path
from . import __version__


@click.group()
def cli() -> None:
    f"""This is used to group the other commands"""
    pass


@click.command()
def explain_upload_args() -> None:
    """Explains the valid arguments for upload_args"""
    show_help.upload_args()


@click.command()
def help() -> None:
    """Information on how to use this tool"""
    print(
        f"You are using version: {__version__}.\n\nFor more complete examples, visit https://github.com/douglassimonsen/redshift_upload"
    )


@click.command()
def add_user() -> None:
    """Starts a cli to create a user for the library"""
    show_add_user.main()


@click.command()
def run_tests() -> None:
    """Runs the test suite"""
    os.chdir(Path(__file__).parents[1])
    pytest.main(["."])


cli.add_command(explain_upload_args)
cli.add_command(help)
cli.add_command(add_user)
cli.add_command(gen_environment.gen_environment)
cli.add_command(run_tests)
if __name__ == "__main__":
    cli()
