from setuptools import setup

setup(
    name="scl-task_base",
    description="Base python classes for creating HII and SCL tasks",
    version="1.0",
    py_modules=["task_base"],
    install_requires=[
        "earthengine-api==0.1.254",
        "gitpython==3.1.14",
        "google-api-python-client==1.12.5",
    ],
)
