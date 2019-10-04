from setuptools import setup

setup(
    name="sweeps",
    packages=["sweeps"],
    entry_points = {
        "console_scripts": [
            "sweeps = sweeps.__main__:main"
        ]
    }
)
