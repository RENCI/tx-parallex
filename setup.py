from glob import glob
from os.path import basename, splitext
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

    setuptools.setup(
        name="tx-parallex",
        version="0.0.97",
        license="MIT",
        author="Hao Xu",
        author_email="xuhao@renci.org",
        description="A job queue with data dependencies",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/RENCI/tx-parallex",
        packages=setuptools.find_namespace_packages("src", exclude=["tests", "tests.*"]),
        package_dir={
            "": "src"
        },
        include_package_data=True,
        py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
        install_requires=[
            "more-itertools==8.2.0",
            "jsonschema==3.2.0",
            "pyyaml==5.3.1",
            "tx-functional>=0.0.16",
            "graph-theory==2020.5.6.39102",
            "terminaltables==3.1.0",
            "jsonpickle==1.4.1",
            "joblib==0.16.0"
        ],
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
        python_requires='>=3.8',
    )



