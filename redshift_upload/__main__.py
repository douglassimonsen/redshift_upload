try:
    from cli import show_help, show_add_user
except ModuleNotFoundError:
    from .cli import show_help, show_add_user
import click


@click.group()
def cli():
    print("hi")
    pass


@click.command()
def upload_args():
    "Shows the valid arguments that can be entered in upload_args"
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


cli.add_command(upload_args)
cli.add_command(help)
cli.add_command(add_user)
if __name__ == "__main__":
    cli()
