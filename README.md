siuba
=====

[![Build Status](https://travis-ci.org/machow/siuba.svg?branch=master)](https://travis-ci.org/machow/siuba)

Installation
------------

```
pip install siuba
```

Examples
--------

* [siu expressions](examples/examples-siu.ipynb)
* [dplyr style pandas](examples/examples-dplyr-funcs.ipynb)
  - [select verb case study](examples/case-iris-select.ipynb)
* [sql using dplyr style](examples/examples-sql.ipynb)
* [tidytuesday examples](https://github.com/machow/tidytuesday-py)
  - tidytuesday is a weekly R data analysis project. In order to kick the tires
    on siuba, I've been using it to complete the assignments. More specifically,
    I've been porting Dave Robinson's [tidytuesday analyses](https://github.com/dgrtwo/data-screencasts)
    to use siuba.

See all [jupyter notebooks here](examples)

Testing
-------

Currently, integration tests are run using nbval and the example notebooks.
These tests can be run using the follwing command.

```
# start postgres db
docker-compose up
make test
```

Note that once things settle down, I'll make sure everything is bolted down
by adding unit tests with pytest.
