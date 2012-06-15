from setuptools import setup, find_packages
import sys

setup(
    name="apollo",
    version = '0.1',
    maintainer='Luminoso, LLC',
    maintainer_email='dev@lumino.so',
    license = "Proprietary",
    url = 'http://github.com/LuminosoInsight/plumbing',
    platforms = ["any"],
    description = "A library for monitoring queues from an Apache Apollo message broker",
    packages=find_packages(),
)
