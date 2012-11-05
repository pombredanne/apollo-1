from setuptools import setup, find_packages

setup(
    name="apollo",
    version = '0.1',
    maintainer='Luminoso, LLC',
    maintainer_email='dev@lumino.so',
    license = "LICENSE",
    url = 'http://github.com/LuminosoInsight/apollo',
    platforms = ["any"],
    description = "A library for monitoring queues from an Apache Apollo message broker",
    packages=find_packages(),
    install_requires=['gevent'],
    entry_points={
        'console_scripts':
        ['apollo-monitor = apollo.cli:start_monitor',
        ]},
)
