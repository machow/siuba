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
| pipe syntax (`>>`)                         | ✅      | ✅         | ❌       |
| concise, **lazy operations** (`_.a + _.b`) | ✅      | ✅         | ❌       |
| operations are Pandas Series methods       | ✅      | ✅         | ✅       |
| operations use **abstract syntax trees**   | ✅      | ❌         | ❌       |
| no need to constantly reset index          | ✅      | ✅         | ❌       |
| **unified API** over (un)grouped data      | ✅      | ✅         | ❌       |
| optimized, **fast grouped operations**     | ✅      | ❌         | ⚠️        |
| generate **SQL queries**                   | ✅      | ❌         | ❌       |
| supports **user defined functions**        | ✅      | ❌         | ⚠️        |
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

<table border="0" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>category</th>      <th>method</th>      <th>fast_pandas</th>      <th>postgresql</th>    </tr>  </thead>  <tbody>    <tr>      <th>0</th>      <td>computations</td>      <td>abs</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>1</th>      <td></td>      <td>all</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>2</th>      <td></td>      <td>any</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>3</th>      <td></td>      <td>between</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>4</th>      <td></td>      <td>clip</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>5</th>      <td></td>      <td>corr</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>6</th>      <td></td>      <td>count</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>7</th>      <td></td>      <td>cov</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>8</th>      <td></td>      <td>cummax</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>9</th>      <td></td>      <td>cummin</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>10</th>      <td></td>      <td>cumprod</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>11</th>      <td></td>      <td>cumsum</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>12</th>      <td></td>      <td>diff</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>13</th>      <td></td>      <td>is_monotonic</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>14</th>      <td></td>      <td>is_monotonic_decreasing</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>15</th>      <td></td>      <td>is_monotonic_increasing</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>16</th>      <td></td>      <td>is_unique</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>17</th>      <td></td>      <td>kurt</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>18</th>      <td></td>      <td>kurtosis</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>19</th>      <td></td>      <td>mad</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>20</th>      <td></td>      <td>max</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>21</th>      <td></td>      <td>mean</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>22</th>      <td></td>      <td>median</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>23</th>      <td></td>      <td>min</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>24</th>      <td></td>      <td>mode</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>25</th>      <td></td>      <td>nunique</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>26</th>      <td></td>      <td>pct_change</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>27</th>      <td></td>      <td>prod</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>28</th>      <td></td>      <td>quantile</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>29</th>      <td></td>      <td>rank</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>30</th>      <td></td>      <td>sem</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>31</th>      <td></td>      <td>skew</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>32</th>      <td></td>      <td>std</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>33</th>      <td></td>      <td>sum</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>34</th>      <td></td>      <td>var</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>35</th>      <td>conversion</td>      <td>astype</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>36</th>      <td></td>      <td>copy</td>      <td>✅</td>      <td>❌</td>    </tr>    <tr>      <th>37</th>      <td>datetime methods</td>      <td>dt.ceil</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>38</th>      <td></td>      <td>dt.day_name</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>39</th>      <td></td>      <td>dt.floor</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>40</th>      <td></td>      <td>dt.month_name</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>41</th>      <td></td>      <td>dt.normalize</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>42</th>      <td></td>      <td>dt.round</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>43</th>      <td></td>      <td>dt.strftime</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>44</th>      <td></td>      <td>dt.to_period</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>45</th>      <td></td>      <td>dt.tz_localize</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>46</th>      <td>datetime properties</td>      <td>dt.date</td>      <td>✅</td>      <td>❌</td>    </tr>    <tr>      <th>47</th>      <td></td>      <td>dt.day</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>48</th>      <td></td>      <td>dt.dayofweek</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>49</th>      <td></td>      <td>dt.dayofyear</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>50</th>      <td></td>      <td>dt.days_in_month</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>51</th>      <td></td>      <td>dt.daysinmonth</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>52</th>      <td></td>      <td>dt.freq</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>53</th>      <td></td>      <td>dt.hour</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>54</th>      <td></td>      <td>dt.is_leap_year</td>      <td>✅</td>      <td>❌</td>    </tr>    <tr>      <th>55</th>      <td></td>      <td>dt.is_month_end</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>56</th>      <td></td>      <td>dt.is_month_start</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>57</th>      <td></td>      <td>dt.is_quarter_end</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>58</th>      <td></td>      <td>dt.is_quarter_start</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>59</th>      <td></td>      <td>dt.is_year_end</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>60</th>      <td></td>      <td>dt.is_year_start</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>61</th>      <td></td>      <td>dt.microsecond</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>62</th>      <td></td>      <td>dt.minute</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>63</th>      <td></td>      <td>dt.month</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>64</th>      <td></td>      <td>dt.nanosecond</td>      <td>✅</td>      <td>❌</td>    </tr>    <tr>      <th>65</th>      <td></td>      <td>dt.quarter</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>66</th>      <td></td>      <td>dt.second</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>67</th>      <td></td>      <td>dt.time</td>      <td>✅</td>      <td>❌</td>    </tr>    <tr>      <th>68</th>      <td></td>      <td>dt.timetz</td>      <td>✅</td>      <td>❌</td>    </tr>    <tr>      <th>69</th>      <td></td>      <td>dt.tz</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>70</th>      <td></td>      <td>dt.week</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>71</th>      <td></td>      <td>dt.weekday</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>72</th>      <td></td>      <td>dt.weekofyear</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>73</th>      <td></td>      <td>dt.year</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>74</th>      <td>indexing</td>      <td>at</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>75</th>      <td></td>      <td>get</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>76</th>      <td></td>      <td>iat</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>77</th>      <td>io</td>      <td>to_json</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>78</th>      <td></td>      <td>to_string</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>79</th>      <td></td>      <td>to_xarray</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>80</th>      <td>missing data</td>      <td>fillna</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>81</th>      <td></td>      <td>isna</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>82</th>      <td></td>      <td>isnull</td>      <td>✅</td>      <td>❌</td>    </tr>    <tr>      <th>83</th>      <td></td>      <td>notna</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>84</th>      <td></td>      <td>notnull</td>      <td>✅</td>      <td>❌</td>    </tr>    <tr>      <th>85</th>      <td>period properties</td>      <td>dt.end_time</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>86</th>      <td></td>      <td>dt.qyear</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>87</th>      <td></td>      <td>dt.start_time</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>88</th>      <td>reindexing</td>      <td>drop_duplicates</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>89</th>      <td></td>      <td>duplicated</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>90</th>      <td></td>      <td>equals</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>91</th>      <td></td>      <td>first</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>92</th>      <td></td>      <td>head</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>93</th>      <td></td>      <td>idxmax</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>94</th>      <td></td>      <td>idxmin</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>95</th>      <td></td>      <td>isin</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>96</th>      <td></td>      <td>mask</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>97</th>      <td></td>      <td>sample</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>98</th>      <td></td>      <td>tail</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>99</th>      <td></td>      <td>where</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>100</th>      <td>reshaping</td>      <td>searchsorted</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>101</th>      <td>string methods</td>      <td>str.capitalize</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>102</th>      <td></td>      <td>str.casefold</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>103</th>      <td></td>      <td>str.cat</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>104</th>      <td></td>      <td>str.center</td>      <td>✅</td>      <td>❌</td>    </tr>    <tr>      <th>105</th>      <td></td>      <td>str.contains</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>106</th>      <td></td>      <td>str.count</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>107</th>      <td></td>      <td>str.decode</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>108</th>      <td></td>      <td>str.encode</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>109</th>      <td></td>      <td>str.endswith</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>110</th>      <td></td>      <td>str.extractall</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>111</th>      <td></td>      <td>str.find</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>112</th>      <td></td>      <td>str.findall</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>113</th>      <td></td>      <td>str.get</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>114</th>      <td></td>      <td>str.index</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>115</th>      <td></td>      <td>str.isalnum</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>116</th>      <td></td>      <td>str.isalpha</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>117</th>      <td></td>      <td>str.isdecimal</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>118</th>      <td></td>      <td>str.isdigit</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>119</th>      <td></td>      <td>str.islower</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>120</th>      <td></td>      <td>str.isnumeric</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>121</th>      <td></td>      <td>str.isspace</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>122</th>      <td></td>      <td>str.istitle</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>123</th>      <td></td>      <td>str.isupper</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>124</th>      <td></td>      <td>str.join</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>125</th>      <td></td>      <td>str.len</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>126</th>      <td></td>      <td>str.ljust</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>127</th>      <td></td>      <td>str.lower</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>128</th>      <td></td>      <td>str.lstrip</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>129</th>      <td></td>      <td>str.match</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>130</th>      <td></td>      <td>str.normalize</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>131</th>      <td></td>      <td>str.pad</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>132</th>      <td></td>      <td>str.repeat</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>133</th>      <td></td>      <td>str.replace</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>134</th>      <td></td>      <td>str.rfind</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>135</th>      <td></td>      <td>str.rindex</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>136</th>      <td></td>      <td>str.rjust</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>137</th>      <td></td>      <td>str.rsplit</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>138</th>      <td></td>      <td>str.rstrip</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>139</th>      <td></td>      <td>str.slice</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>140</th>      <td></td>      <td>str.slice_replace</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>141</th>      <td></td>      <td>str.split</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>142</th>      <td></td>      <td>str.startswith</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>143</th>      <td></td>      <td>str.strip</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>144</th>      <td></td>      <td>str.swapcase</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>145</th>      <td></td>      <td>str.title</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>146</th>      <td></td>      <td>str.translate</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>147</th>      <td></td>      <td>str.upper</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>148</th>      <td></td>      <td>str.wrap</td>      <td>✅</td>      <td>✅</td>    </tr>    <tr>      <th>149</th>      <td></td>      <td>str.zfill</td>      <td>🚧</td>      <td></td>    </tr>    <tr>      <th>150</th>      <td>time series</td>      <td>asof</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>151</th>      <td></td>      <td>at_time</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>152</th>      <td></td>      <td>between_time</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>153</th>      <td></td>      <td>first_valid_index</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>154</th>      <td></td>      <td>last_valid_index</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>155</th>      <td></td>      <td>resample</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>156</th>      <td></td>      <td>shift</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>157</th>      <td></td>      <td>slice_shift</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>158</th>      <td></td>      <td>tshift</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>159</th>      <td></td>      <td>tz_convert</td>      <td>🤔</td>      <td></td>    </tr>    <tr>      <th>160</th>      <td></td>      <td>tz_localize</td>      <td>🤔</td>      <td></td>    </tr>  </tbody></table>

Testing
-------

Tests are done using pytest.
They can be run using the following.

```bash
# start postgres db
docker-compose up
pytest siuba
```
