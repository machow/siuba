from setuptools import setup, find_packages

# parse version ---------------------------------------------------------------

import re
import ast
_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('siuba/__init__.py', 'rb') as f:
    VERSION = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

with open('README.md') as f:
    README = f.read()

# setup -----------------------------------------------------------------------

setup(
    name='siuba',
    packages=find_packages(),
    version=VERSION,
    description='A package for quick, scrappy analyses with pandas and SQL',
    author='Michael Chow',
    license='MIT',
    author_email='mc_al_gh_siuba@fastmail.com',
    url='https://github.com/machow/siuba',
    keywords=['package', ],
    install_requires=[
        "pandas>=0.24.0",
        "numpy>=1.12.0",
        "SQLAlchemy>=1.2.19",
        "PyYAML>=3.0.0"
    ],
    python_requires=">=3.6",
    include_package_data=True,
    long_description=README,
    long_description_content_type="text/markdown",
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)

