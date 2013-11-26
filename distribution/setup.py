from setuptools import setup

setup(
    name='6450-distfs',
    version='0.1.0',
    author='Nick Kiermaier, Chad Ross, Tom Sires',
    author_email='nickthemagicman@live.com',
    packages=['distfs','distfs.chunk','distfs.remotefs'],
    scripts=[],
    url='http://pypi.python.org/pypi/tbd/',
    license='LICENSE.txt',
    description='Csci 6450 project-distributed filesystem',
    long_description=open('docs/README.txt').read(),
    install_requires=[
        "pyzmq",
        "kazoo",
        "msgpack-python",
        "pymongo",
        "fusepy"
    ],
     entry_points={
        'console_scripts':
            ['distfsMount = distfs.distfs:distfsMount']
        }
)


"""
check the setup.py for validity-> python setup.py check



pip list ->lists all pip packages
python setup.py sdist -> generate distribution package
sudo pip install <package>
sudo pip uninstall <package>
"""