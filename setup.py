from setuptools import setup, find_packages

# parse version ---------------------------------------------------------------

import re
import ast
_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('siuba/__init__.py', 'rb') as f:
    VERSION = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

with open('README.md', encoding="utf-8") as f:
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
        "pandas>=0.24.0,<2.1.0",
        "numpy>=1.12.0",
        "SQLAlchemy>=1.2.19",
        "PyYAML>=3.0.0"
    ],
    extras_require={
        "test": [
            "pytest",
            "hypothesis",
            "IPython",
            "pymysql",
            "psycopg2-binary",
            "duckdb_engine",
            # duckdb 0.8.0 has a bug which always errors for pandas v2+
            # it's been fixed, but we need to pin until duckdb v0.9.0
            "duckdb",
        ],
        "docs": [
            "plotnine",
            "jupyter",
            "nbval",
            "sphinx",
            "nbsphinx",
            "jupytext",
            "gapminder==0.1",
        ],
    },
    python_requires=">=3.7",
    include_package_data=True,
    long_description=README,
    long_description_content_type="text/markdown",
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)

