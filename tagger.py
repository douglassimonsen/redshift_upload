import os, sys

this_directory = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
sys.path.insert(0, this_directory)

from redshift_upload import __version__
import git

possible_tag = "v" + __version__

repo = git.Repo()
if possible_tag not in repo.tags:
    tag = repo.create_tag(
        possible_tag, message=f"Automatic Tag for release: {possible_tag}"
    )
    repo.remotes.origin.push(tag)
repo.remotes.origin.push()
