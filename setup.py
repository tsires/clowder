from setuptools import setup

setup(
    name='clowder',
    version='0.1.4',
    author='Nick Kiermaier, Chad Ross, Tom Sires',
    author_email='nickthemagicman@live.com',
    packages=['clowder'],
    scripts=[],
    url='https://gitlab.cs.uno.edu/tsires/distfs',
    license='LICENSE.txt',
    description='ClowderFS: Distributed Filesystem project for CSCI 6450',
    long_description=open('README').read(),
    install_requires=[
        "pyzmq",
        "kazoo",
        "msgpack-python",
        "fusepy",
    ],
    dependency_links=[
        "git+https://github.com/tsires/fusepy.git#fusepy",
    ],
    entry_points={
        'console_scripts':[
		'mkfs.clowder = clowder.cli:mkfs',
		'mount.clowder = clowder.cli:mount',
		]
        }
)


"""
check the setup.py for validity-> python setup.py check



pip list ->lists all pip packages
python setup.py sdist -> generate distribution package
sudo pip install <package>
sudo pip uninstall <package>
"""
