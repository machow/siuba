siuba
=====

[![Build Status](https://travis-ci.org/machow/siuba.svg?branch=master)](https://travis-ci.org/machow/siuba)
[![Documentation Status](https://readthedocs.org/projects/siuba/badge/?version=latest)](https://siuba.readthedocs.io/en/latest/?badge=latest)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/machow/siuba/master)

**Note: this library is currently in heavy development! For a nice example of siuba in action, see this [intro doc](https://siuba.readthedocs.io/en/latest/intro.html), or this [example analysis](https://github.com/machow/tidytuesday-py/blob/master/2019-01-08-tv-golden-age.ipynb)**

Installation
------------

```
pip install siuba
```

Examples
--------

See the [siuba docs](https://siuba.readthedocs.io) for a full introduction.

### Basic use

```python
from siuba import *
from siuba.data import mtcars

(mtcars
  >> group_by(_.cyl)
  >> summarize(avg_hp = _.hp.mean())
  )
```

```
Out[1]: 
   cyl      avg_hp
0    4   82.636364
1    6  122.285714
2    8  209.214286
```

See [introduction to siuba](https://siuba.readthedocs.io/en/latest/intro.html#Introduction-to-siuba).

### What is a siu expression (e.g. `_.cyl == 4`)?

```python
from siuba import _

# old approach
mtcars[lambda _: _.cyl == 4]

# siu approach
mtcars[_.cyl == 4]
```

```
Out[2]: 
     mpg  cyl   disp   hp  drat     wt   qsec  vs  am  gear  carb
2   22.8    4  108.0   93  3.85  2.320  18.61   1   1     4     1
7   24.4    4  146.7   62  3.69  3.190  20.00   1   0     4     2
..   ...  ...    ...  ...   ...    ...    ...  ..  ..   ...   ...
27  30.4    4   95.1  113  3.77  1.513  16.90   1   1     5     2
31  21.4    4  121.0  109  4.11  2.780  18.60   1   1     4     2

[11 rows x 11 columns]
```

See [siu expression section here](https://siuba.readthedocs.io/en/latest/intro.html#Concise-pandas-operations-with-siu-expressions-(_)).

### Using with SQL

```python
from siuba import _, group_by, summarize, filter
from siuba.data import mtcars

from sqlalchemy import create_engine
from siuba.sql import LazyTbl

# copy in to sqlite
engine = create_engine("sqlite:///:memory:")
mtcars.to_sql("mtcars", engine, if_exists = "replace")

# connect with siuba
tbl_mtcars = LazyTbl(engine, "mtcars")

(tbl_mtcars
  >> group_by(_.cyl)
  >> summarize(avg_hp = _.hp.mean())
  )
```

```
Out[3]: 
# Source: lazy query
# DB Conn: Engine(sqlite:///:memory:)
# Preview:
   cyl      avg_hp
0    4   82.636364
1    6  122.285714
2    8  209.214286
# .. may have more rows
```

See [querying SQL introduction here](https://siuba.readthedocs.io/en/latest/intro_sql_basic.html).

### Example notebooks

Below are some examples I've kept as I've worked on siuba.
For the most up to date explanations, see the [siuba docs](https://siuba.readthedocs.io)

* [siu expressions](examples/examples-siu.ipynb)
* [dplyr style pandas](examples/examples-dplyr-funcs.ipynb)
  - [select verb case study](examples/case-iris-select.ipynb)
* sql using dplyr style
  - [simple sql statements](examples/examples-sql.ipynb)
  - [the kitchen sink with postgres](examples/examples-postgres.ipynb)
* [tidytuesday examples](https://github.com/machow/tidytuesday-py)
  - tidytuesday is a weekly R data analysis project. In order to kick the tires
    on siuba, I've been using it to complete the assignments. More specifically,
    I've been porting Dave Robinson's [tidytuesday analyses](https://github.com/dgrtwo/data-screencasts)
    to use siuba.

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
