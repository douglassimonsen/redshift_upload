try:
    from cli import show_help, show_add_user
    from cli.gen_environment import gen_environment
except ModuleNotFoundError:
    from .cli import show_help, show_add_user
    from .cli.gen_environment import gen_environment
import click
import pytest
import os


@click.group()
def cli():
    pass


@click.command()
def explain_upload_args():
    "Explains the valid arguments for upload_args"
    show_help.upload_args()


@click.command()
def help():
    "Information on how to use this tool"
    print(
        "For more complete examples, visit https://github.com/douglassimonsen/redshift_upload"
    )


@click.command()
def add_user():
    "Starts a cli to create a user for the library"
    show_add_user.main()


@click.command()
def run_tests():
    "Runs the test suite"
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    pytest.main(["."])


cli.add_command(explain_upload_args)
cli.add_command(help)
cli.add_command(add_user)
cli.add_command(gen_environment.gen_environment)
cli.add_command(run_tests)
if __name__ == "__main__":
    cli()
