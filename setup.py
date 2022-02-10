import setuptools
import sys
from pathlib import Path

sys.path.insert(0, Path(__file__).parent)
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
        "click",
        "colorama",
        "jsonschema",
        "pandas",
        "psycopg2",
        "pytest",
        "toposort",
    ],
    long_description=(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
)
