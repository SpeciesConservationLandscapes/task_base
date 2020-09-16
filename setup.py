from setuptools import setup

setup(
    name="scl-task_base",
    description="Base python classes for creating HII and SCL tasks",
    version="0.51",
    py_modules=["task_base"],
    install_requires=["earthengine-api"],
)
