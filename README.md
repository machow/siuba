siuba
=====

*scrappy data analysis, with seamless support for pandas and SQL*

[![Build Status](https://travis-ci.org/machow/siuba.svg?branch=master)](https://travis-ci.org/machow/siuba)
[![Documentation Status](https://readthedocs.org/projects/siuba/badge/?version=latest)](https://siuba.readthedocs.io/en/latest/?badge=latest)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/machow/siuba/master)

<img width="30%" align="right" src="./docs/siuba_small.svg">

siuba is a port of [dplyr](https://github.com/tidyverse/dplyr) and other R libraries. It supports a tabular data analysis workflow centered on 5 common actions:

* `select()` - keep certain columns of data.
* `filter()` - keep certain rows of data.
* `mutate()` - create or modify an existing column of data.
* `summarize()` - reduce one or more columns down to a single number.
* `arrange()` - reorder the rows of data.

These actions can be preceeded by a `group_by()`, which causes them to be applied individually to grouped rows of data. Moreover, many SQL concepts, such as `distinct()`, `count()`, and joins are implemented.
Inputs to these functions can be a pandas `DataFrame` or SQL connection (currently postgres, redshift, or sqlite).

For more on the rationale behind tools like dplyr, see this [tidyverse paper](https://tidyverse.tidyverse.org/articles/paper.html). 
For examples of siuba in action, see the [siuba documentation](https://siuba.readthedocs.io/en/latest/intro.html).

Installation
------------

```
pip install siuba
```

<br>

<table>
  <tbody>
    <tr>
      <th colspan=3>Jump to Section...</td>
    </tr>
    <tr>
      <td align="center"><a href="#key-features">Key Features</a><br>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>    
        <span>&nbsp;&nbsp;</span>
      </td>
      <td align="center"><a href="#examples">Examples</a><br>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>        
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;</span>
      </td>
      <td align="center"><a href="#supported-methods">Supported Methods</a><br>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>    
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
        <span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>     
        <span>&nbsp;&nbsp;</span>        
      </td>
    </tr>
  </tbody>
</table>

Key Features
------------

| feature                                    | siuba   | dplython   | pandas   |
|:-------------------------------------------|:--------|:-----------|:---------|
| operations are Pandas Series methods       | ✅      | ✅         | ✅       |
| supports **user defined functions**        | ✅      | ✅         | ✅       |
| pipe syntax (`>>`)                         | ✅      | ✅         | ❌       |
| concise, **lazy operations** (`_.a + _.b`) | ✅      | ✅         | ❌       |
| no need to constantly reset index          | ✅      | ✅         | ❌       |
| **unified API** over (un)grouped data      | ✅      | ✅         | ❌       |
| optimized, **fast grouped operations**     | ✅      | ❌         | ✅       |
| generate **SQL queries**                   | ✅      | ❌         | ❌       |
| operations use **abstract syntax trees**   | ✅      | ❌         | ❌       |
| handles nested data                        | ✅      | ❌         | ⚠️        |

**For more details, see [key features and benchmarks](https://siuba.readthedocs.io/en/latest/).**

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

You can also think siu expressions as a shorthand for a lambda function.

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

### Using with SQL

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


Supported Methods
------------------------

<table><tbody><tr><th>method</th><th>fast pandas</th><th>postgresql</th></tr><tr><td><b>computations<b></td><td></td><td></td></tr><tr><td>all</td><td>✅</td><td>✅</td></tr><tr><td>any</td><td>✅</td><td>✅</td></tr><tr><td>between</td><td>✅</td><td>✅</td></tr><tr><td>clip</td><td>✅</td><td>✅</td></tr><tr><td>corr</td><td>🚧</td><td></td></tr><tr><td>count</td><td>✅</td><td>✅</td></tr><tr><td>cov</td><td>🚧</td><td></td></tr><tr><td>cummax</td><td>✅</td><td>✅</td></tr><tr><td>cummin</td><td>✅</td><td>✅</td></tr><tr><td>cumprod</td><td>✅</td><td>✅</td></tr><tr><td>cumsum</td><td>✅</td><td>✅</td></tr><tr><td>diff</td><td>✅</td><td>✅</td></tr><tr><td>is_monotonic</td><td>🚧</td><td></td></tr><tr><td>is_monotonic_decreasing</td><td>🚧</td><td></td></tr><tr><td>is_monotonic_increasing</td><td>🚧</td><td></td></tr><tr><td>is_unique</td><td>🚧</td><td></td></tr><tr><td>kurt</td><td>🚧</td><td></td></tr><tr><td>kurtosis</td><td>🚧</td><td></td></tr><tr><td>mad</td><td>✅</td><td>✅</td></tr><tr><td>max</td><td>✅</td><td>✅</td></tr><tr><td>mean</td><td>✅</td><td>✅</td></tr><tr><td>median</td><td>✅</td><td>✅</td></tr><tr><td>min</td><td>✅</td><td>✅</td></tr><tr><td>mode</td><td>🚧</td><td></td></tr><tr><td>nunique</td><td>✅</td><td>✅</td></tr><tr><td>pct_change</td><td>✅</td><td>✅</td></tr><tr><td>prod</td><td>✅</td><td>✅</td></tr><tr><td>quantile</td><td>✅</td><td>✅</td></tr><tr><td>rank</td><td>✅</td><td>✅</td></tr><tr><td>sem</td><td>✅</td><td>✅</td></tr><tr><td>skew</td><td>✅</td><td>✅</td></tr><tr><td>std</td><td>✅</td><td>✅</td></tr><tr><td>sum</td><td>✅</td><td>✅</td></tr><tr><td>var</td><td>✅</td><td>✅</td></tr><tr><td><b>conversion<b></td><td></td><td></td></tr><tr><td>copy</td><td>✅</td><td>❌</td></tr><tr><td><b>datetime methods<b></td><td></td><td></td></tr><tr><td>dt.day_name</td><td>✅</td><td>✅</td></tr><tr><td>dt.floor</td><td>✅</td><td>✅</td></tr><tr><td>dt.month_name</td><td>✅</td><td>✅</td></tr><tr><td>dt.normalize</td><td>✅</td><td>✅</td></tr><tr><td>dt.round</td><td>✅</td><td>✅</td></tr><tr><td>dt.strftime</td><td>✅</td><td>✅</td></tr><tr><td>dt.to_period</td><td>✅</td><td>✅</td></tr><tr><td>dt.tz_localize</td><td>✅</td><td>✅</td></tr><tr><td><b>datetime properties<b></td><td></td><td></td></tr><tr><td>dt.day</td><td>✅</td><td>✅</td></tr><tr><td>dt.dayofweek</td><td>✅</td><td>✅</td></tr><tr><td>dt.dayofyear</td><td>✅</td><td>✅</td></tr><tr><td>dt.days_in_month</td><td>✅</td><td>✅</td></tr><tr><td>dt.daysinmonth</td><td>✅</td><td>✅</td></tr><tr><td>dt.freq</td><td>✅</td><td>✅</td></tr><tr><td>dt.hour</td><td>✅</td><td>✅</td></tr><tr><td>dt.is_leap_year</td><td>✅</td><td>❌</td></tr><tr><td>dt.is_month_end</td><td>✅</td><td>✅</td></tr><tr><td>dt.is_month_start</td><td>✅</td><td>✅</td></tr><tr><td>dt.is_quarter_end</td><td>✅</td><td>✅</td></tr><tr><td>dt.is_quarter_start</td><td>✅</td><td>✅</td></tr><tr><td>dt.is_year_end</td><td>✅</td><td>✅</td></tr><tr><td>dt.is_year_start</td><td>✅</td><td>✅</td></tr><tr><td>dt.microsecond</td><td>✅</td><td>✅</td></tr><tr><td>dt.minute</td><td>✅</td><td>✅</td></tr><tr><td>dt.month</td><td>✅</td><td>✅</td></tr><tr><td>dt.nanosecond</td><td>✅</td><td>❌</td></tr><tr><td>dt.quarter</td><td>✅</td><td>✅</td></tr><tr><td>dt.second</td><td>✅</td><td>✅</td></tr><tr><td>dt.time</td><td>✅</td><td>❌</td></tr><tr><td>dt.timetz</td><td>✅</td><td>❌</td></tr><tr><td>dt.tz</td><td>✅</td><td>✅</td></tr><tr><td>dt.week</td><td>✅</td><td>✅</td></tr><tr><td>dt.weekday</td><td>✅</td><td>✅</td></tr><tr><td>dt.weekofyear</td><td>✅</td><td>✅</td></tr><tr><td>dt.year</td><td>✅</td><td>✅</td></tr><tr><td><b>indexing<b></td><td></td><td></td></tr><tr><td>get</td><td>🚧</td><td></td></tr><tr><td>iat</td><td>🚧</td><td></td></tr><tr><td><b>io<b></td><td></td><td></td></tr><tr><td>to_string</td><td>🤔</td><td></td></tr><tr><td>to_xarray</td><td>🤔</td><td></td></tr><tr><td><b>missing data<b></td><td></td><td></td></tr><tr><td>isna</td><td>✅</td><td>✅</td></tr><tr><td>isnull</td><td>✅</td><td>❌</td></tr><tr><td>notna</td><td>✅</td><td>✅</td></tr><tr><td>notnull</td><td>✅</td><td>❌</td></tr><tr><td><b>period properties<b></td><td></td><td></td></tr><tr><td>dt.qyear</td><td>🤔</td><td></td></tr><tr><td>dt.start_time</td><td>🤔</td><td></td></tr><tr><td><b>reindexing<b></td><td></td><td></td></tr><tr><td>duplicated</td><td>🚧</td><td></td></tr><tr><td>equals</td><td>🚧</td><td></td></tr><tr><td>first</td><td>🤔</td><td></td></tr><tr><td>head</td><td>🤔</td><td></td></tr><tr><td>idxmax</td><td>🤔</td><td></td></tr><tr><td>idxmin</td><td>🤔</td><td></td></tr><tr><td>isin</td><td>✅</td><td>✅</td></tr><tr><td>mask</td><td>🚧</td><td></td></tr><tr><td>sample</td><td>🤔</td><td></td></tr><tr><td>tail</td><td>🤔</td><td></td></tr><tr><td>where</td><td>🚧</td><td></td></tr><tr><td><b>reshaping<b></td><td></td><td></td></tr><tr><td><b>string methods<b></td><td></td><td></td></tr><tr><td>str.casefold</td><td>🚧</td><td></td></tr><tr><td>str.cat</td><td>🚧</td><td></td></tr><tr><td>str.center</td><td>✅</td><td>❌</td></tr><tr><td>str.contains</td><td>✅</td><td>✅</td></tr><tr><td>str.count</td><td>✅</td><td>✅</td></tr><tr><td>str.decode</td><td>🚧</td><td></td></tr><tr><td>str.encode</td><td>✅</td><td>✅</td></tr><tr><td>str.endswith</td><td>✅</td><td>✅</td></tr><tr><td>str.extractall</td><td>🤔</td><td></td></tr><tr><td>str.find</td><td>✅</td><td>✅</td></tr><tr><td>str.findall</td><td>✅</td><td>✅</td></tr><tr><td>str.get</td><td>🚧</td><td></td></tr><tr><td>str.index</td><td>🚧</td><td></td></tr><tr><td>str.isalnum</td><td>✅</td><td>✅</td></tr><tr><td>str.isalpha</td><td>✅</td><td>✅</td></tr><tr><td>str.isdecimal</td><td>✅</td><td>✅</td></tr><tr><td>str.isdigit</td><td>✅</td><td>✅</td></tr><tr><td>str.islower</td><td>✅</td><td>✅</td></tr><tr><td>str.isnumeric</td><td>✅</td><td>✅</td></tr><tr><td>str.isspace</td><td>✅</td><td>✅</td></tr><tr><td>str.istitle</td><td>✅</td><td>✅</td></tr><tr><td>str.isupper</td><td>✅</td><td>✅</td></tr><tr><td>str.join</td><td>🚧</td><td></td></tr><tr><td>str.len</td><td>✅</td><td>✅</td></tr><tr><td>str.ljust</td><td>✅</td><td>✅</td></tr><tr><td>str.lower</td><td>✅</td><td>✅</td></tr><tr><td>str.lstrip</td><td>✅</td><td>✅</td></tr><tr><td>str.match</td><td>✅</td><td>✅</td></tr><tr><td>str.normalize</td><td>🚧</td><td></td></tr><tr><td>str.pad</td><td>✅</td><td>✅</td></tr><tr><td>str.repeat</td><td>🚧</td><td></td></tr><tr><td>str.replace</td><td>✅</td><td>✅</td></tr><tr><td>str.rfind</td><td>✅</td><td>✅</td></tr><tr><td>str.rindex</td><td>🚧</td><td></td></tr><tr><td>str.rjust</td><td>✅</td><td>✅</td></tr><tr><td>str.rsplit</td><td>✅</td><td>✅</td></tr><tr><td>str.rstrip</td><td>✅</td><td>✅</td></tr><tr><td>str.slice</td><td>✅</td><td>✅</td></tr><tr><td>str.slice_replace</td><td>✅</td><td>✅</td></tr><tr><td>str.split</td><td>✅</td><td>✅</td></tr><tr><td>str.startswith</td><td>✅</td><td>✅</td></tr><tr><td>str.strip</td><td>✅</td><td>✅</td></tr><tr><td>str.swapcase</td><td>✅</td><td>✅</td></tr><tr><td>str.title</td><td>✅</td><td>✅</td></tr><tr><td>str.translate</td><td>🤔</td><td></td></tr><tr><td>str.upper</td><td>✅</td><td>✅</td></tr><tr><td>str.wrap</td><td>✅</td><td>✅</td></tr><tr><td>str.zfill</td><td>🚧</td><td></td></tr><tr><td><b>time series<b></td><td></td><td></td></tr><tr><td>at_time</td><td>🤔</td><td></td></tr><tr><td>between_time</td><td>🤔</td><td></td></tr><tr><td>first_valid_index</td><td>🤔</td><td></td></tr><tr><td>last_valid_index</td><td>🤔</td><td></td></tr><tr><td>resample</td><td>🤔</td><td></td></tr><tr><td>shift</td><td>🤔</td><td></td></tr><tr><td>slice_shift</td><td>🤔</td><td></td></tr><tr><td>tshift</td><td>🤔</td><td></td></tr><tr><td>tz_convert</td><td>🤔</td><td></td></tr><tr><td>tz_localize</td><td>🤔</td><td></td></tr></tbody></table>

**For more details, see the [interactive support table](https://siuba.readthedocs.io/en/latest/).**

Testing
-------

Tests are done using pytest.
They can be run using the following.

```bash
# start postgres db
docker-compose up
pytest siuba
```
