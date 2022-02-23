from setuptools import setup
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="scl-task_base",
    version="1.0.5",
    packages=["task_base"],
    install_requires=[
        "earthengine-api==0.1.254",
        "gitpython==3.1.14",
        "google-api-python-client==1.12.5",
    ],
    description="Base python classes for creating HII and SCL tasks",
    long_description=long_description,
    long_description_content_type="text/markdown",
)
