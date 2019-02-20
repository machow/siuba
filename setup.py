from setuptools import setup, find_packages
setup(
    name='siuba',
    packages=find_packages(),
    version='0.0.2',
    description='Cookiecutter template for a Python package',
    author='Michael Chow',
    license='BSD',
    author_email='mc_al_gh_siuba@fastmail.com',
    url='https://github.com/machow/siuba',
    keywords=['package', ],
    install_requires = [
        "pandas"
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)

