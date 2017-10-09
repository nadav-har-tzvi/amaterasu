from distutils.core import setup, DistutilsSetupError
from ctypes.util import find_library
import os

java_code = os.system('java -version')
if java_code != 0:
    raise DistutilsSetupError("Java is not installed")

libffi_exists = find_library('ffi')
libgit2_exists = find_library('git2') and find_library('git2').endswith('26')

if not libffi_exists:
    raise DistutilsSetupError("libffi is required, please install it and try again")

if not libgit2_exists:
    raise DistutilsSetupError("libgit2 v26 is required, please install it and try again")

setup(
    name='amaterasu',
    version='0.1.0.dev1',
    packages=['', 'amaterasu', 'tests', 'tests.ama_init', 'amaterasu.resources'],
    url='https://github.com/shintoio/amaterasu',
    license='Apache License 2.0 ',
    author='Yaniv Rodenski, Nadav Har Tzvi, Eyal Ben Ivri',
    description='Apache Amaterasu (Incubating) is an open source, configuration managment and deployment framework for data pipelines',
    install_requires=['colorama', 'pygit2', 'six'],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, <4',
    entry_points={
        'console_scripts': [
            'ama = amaterasu.__main__:main'
        ]
    },
    package_data={
        'amaterasu.resources': ['*']
    }
)
