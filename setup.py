import setuptools
import sys, os

this_directory = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
sys.path.insert(0, this_directory)
from redshift_upload import __version__


setuptools.setup(
    name="simple_redshift_upload",
    packages=setuptools.find_packages(),
    version=__version__,
    description="A package that simplifies uploading data to redshift",
    url="https://github.com/mwhamilton/redshift_upload",
    download_url=f"https://github.com/mwhamilton/redshift_upload/archive/{__version__}.tar.gz",
    author="Matthew Hamilton",
    author_email="mwhamilton6@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    include_package_data=True,
    install_requires=[
        "boto3",
        "boto3-stubs[s3]",
        "pandas",
        "psycopg2",
        "toposort",
    ],
)
