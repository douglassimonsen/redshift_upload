import setuptools


setuptools.setup(
    name="simple_redshift_upload",
    packages=["redshift_upload"],
    version="1.1.1",
    description="A package that simplifies uploading data to redshift",
    url="https://github.com/mwhamilton/redshift_upload",
    download_url="https://github.com/mwhamilton/redshift_upload/archive/1.0.0.tar.gz",
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
