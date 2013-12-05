from setuptools import setup

setup(
    name='clowder',
    version='0.2.1',
    author='Nick Kiermaier, Chad Ross, Tom Sires',
    author_email='tsires@uno.edu',
    packages=['clowder'],
    scripts=[],
    url='https://gitlab.cs.uno.edu/tsires/clowder',
    license='LICENSE.txt',
    description='ClowderFS: Distributed Filesystem project for CSCI 6450',
    long_description=open('README').read(),
    install_requires=[
        "kazoo",
        "msgpack-python",
        "fusepy",
        "mmh3",
	"flask",
	"requests",
    ],
    dependency_links=[
        "git+https://github.com/tsires/fusepy.git#fusepy",
    ],
    entry_points={
        'console_scripts':[
		'mkfs.clowder = clowder.cli:mkfs',
		'mount.clowder = clowder.cli:mount',
		'chunkserver.clowder = clowder.cli:chunk',
		]
        }
)
