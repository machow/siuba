siuba
=====

*scrappy data analysis, with seamless support for pandas and SQL*

[![CI](https://github.com/machow/siuba/workflows/CI/badge.svg)](https://github.com/machow/siuba/actions?query=workflow%3ACI+branch%3Amaster)
[![Documentation Status](https://readthedocs.org/projects/siuba/badge/?version=latest)](https://siuba.readthedocs.io/en/latest/?badge=latest)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/machow/siuba/master)

<img width="30%" align="right" src="./docs/siuba_small.svg">

siuba ([小巴](http://www.cantonese.sheik.co.uk/dictionary/words/9139/)) is a port of [dplyr](https://github.com/tidyverse/dplyr) and other R libraries. It supports a tabular data analysis workflow centered on 5 common actions:

* `select()` - keep certain columns of data.
* `filter()` - keep certain rows of data.
* `mutate()` - create or modify an existing column of data.
* `summarize()` - reduce one or more columns down to a single number.
* `arrange()` - reorder the rows of data.

These actions can be preceded by a `group_by()`, which causes them to be applied individually to grouped rows of data. Moreover, many SQL concepts, such as `distinct()`, `count()`, and joins are implemented.
Inputs to these functions can be a pandas `DataFrame` or SQL connection (currently postgres, redshift, or sqlite).

For more on the rationale behind tools like dplyr, see this [tidyverse paper](https://tidyverse.tidyverse.org/articles/paper.html). 
For examples of siuba in action, see the [siuba documentation](https://siuba.readthedocs.io/en/latest/intro.html).

Installation
------------

```
pip install siuba
```

Examples
--------

See the [siuba docs](https://siuba.readthedocs.io) or this [live analysis](https://www.youtube.com/watch?v=eKuboGOoP08) for a full introduction.

### Basic use

The code below uses the example DataFrame `mtcars`, to get the average horsepower (hp) per cylinder.

```python
from siuba import group_by, summarize, _
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

There are three key concepts in this example:

| concept | example | meaning |
| ------- | ------- | ------- |
| verb    | `group_by(...)` | a function that operates on a table, like a DataFrame or SQL table |
| siu expression | `_.hp.mean()` | an expression created with `siuba._`, that represents actions you want to perform |
| pipe | `mtcars >> group_by(...)` | a syntax that allows you to chain verbs with the `>>` operator |


See [introduction to siuba](https://siuba.readthedocs.io/en/latest/intro.html#Introduction-to-siuba).

### What is a siu expression (e.g. `_.cyl == 4`)?

A siu expression is a way of specifying **what** action you want to perform.
This allows siuba verbs to decide **how** to execute the action, depending on whether your data is a local DataFrame or remote table.

```python
from siuba import _

_.cyl == 4
```

```
Out[2]:
█─==
├─█─.
│ ├─_
│ └─'cyl'
└─4
```

You can also think of siu expressions as a shorthand for a lambda function.

```python
from siuba import _

# lambda approach
mtcars[lambda _: _.cyl == 4]

# siu expression approach
mtcars[_.cyl == 4]
```

```
Out[3]: 
     mpg  cyl   disp   hp  drat     wt   qsec  vs  am  gear  carb
2   22.8    4  108.0   93  3.85  2.320  18.61   1   1     4     1
7   24.4    4  146.7   62  3.69  3.190  20.00   1   0     4     2
..   ...  ...    ...  ...   ...    ...    ...  ..  ..   ...   ...
27  30.4    4   95.1  113  3.77  1.513  16.90   1   1     5     2
31  21.4    4  121.0  109  4.11  2.780  18.60   1   1     4     2

[11 rows x 11 columns]
```

See [siu expression section here](https://siuba.readthedocs.io/en/latest/intro.html#Concise-pandas-operations-with-siu-expressions-(_)).

### Using with a SQL database

A killer feature of siuba is that the same analysis code can be run on a local DataFrame, or a SQL source.

In the code below, we set up an example database.

```python
# Setup example data ----
from sqlalchemy import create_engine
from siuba.data import mtcars

# copy pandas DataFrame to sqlite
engine = create_engine("sqlite:///:memory:")
mtcars.to_sql("mtcars", engine, if_exists = "replace")
```

Next, we use the code from the first example, except now executed a SQL table.

```python
# Demo SQL analysis with siuba ----
from siuba import _, group_by, summarize, filter
from siuba.sql import LazyTbl

# connect with siuba
tbl_mtcars = LazyTbl(engine, "mtcars")

(tbl_mtcars
  >> group_by(_.cyl)
  >> summarize(avg_hp = _.hp.mean())
  )
```

```
Out[4]: 
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

Tests are done using pytest.
They can be run using the following.

```bash
# start postgres db
docker-compose up
pytest siuba
```
