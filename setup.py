from setuptools import setup, find_packages
setup(
    name='siuba',
    packages=find_packages(),
    version='0.0.6',
    description='A package for quick, scrappy analyses with pandas and SQL',
    author='Michael Chow',
    license='BSD',
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

