# -*- coding: utf-8 -*-

import re
import sys
import os
from setuptools import setup, find_packages


def _get_version():
    v_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'dpq', '__init__.py')
    ver_info_str = re.compile(r".*version_info = \((.*?)\)", re.S). \
        match(open(v_file_path).read()).group(1)
    return re.sub(r'(\'|"|\s+)', '', ver_info_str).replace(',', '.')


def get_dependencies():
    deps = ['redis >= 2.7.0', 'click >= 3.0']
    if sys.version_info < (2, 7) or \
            (sys.version_info >= (3, 0) and sys.version_info < (3, 1)):
        deps += ['importlib']
    if sys.version_info < (2, 7) or \
            (sys.version_info >= (3, 0) and sys.version_info < (3, 2)):
        deps += ['argparse']
    return deps

setup(
    name='dpq',
    version=_get_version(),
    url='https://github.com/DiggerPlus/DPQ',
    license='MIT',
    author='DiggerPlus',
    author_email='diggerplus@163.com',
    long_description=__doc__,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=get_dependencies(),
    extras_require={
        ':python_version=="2.6"': ['argparse', 'importlib'],
    },
)
