import os

from setuptools import find_packages
from setuptools import setup

with open("requirements.txt", "r") as f:
    requirements = f.read().split("\n")
requirements = [r for r in requirements if r != ""]


setup(
    name="carbon_bombs",
    # version=VERSION,
    # maintainer=MAINTAINER,
    # maintainer_email=MAINTAINER_EMAIL,
    # description=DESCRIPTION,
    # long_description=LONG_DESCRIPTION,
    # license=LICENSE,
    url="https://github.com/dataforgoodfr/CarbonBombs",
    packages=find_packages(exclude=["*tests", "tests*", "tests"]),
    include_package_data=True,
    install_requires=requirements,
)
