from setuptools import setup, find_packages

# parse version ---------------------------------------------------------------

import re
import ast
_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('siuba/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

# setup -----------------------------------------------------------------------

setup(
    name='siuba',
    packages=find_packages(),
    version=version,
    description='A package for quick, scrappy analyses with pandas and SQL',
    author='Michael Chow',
    license='MIT',
    author_email='mc_al_gh_siuba@fastmail.com',
    url='https://github.com/machow/siuba',
    keywords=['package', ],
    install_requires = [
        "pandas"
    ],
    include_package_data=True,
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)

