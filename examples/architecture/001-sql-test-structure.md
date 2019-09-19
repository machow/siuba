---
jupyter:
  jupytext:
    text_representation:
      extension: .Rmd
      format_name: rmarkdown
      format_version: '1.1'
      jupytext_version: 1.1.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region -->
# SQL testing: research and key decisions

---

## purpose

siuba's SQL functionality needs to be tested.
Since features are heavily inspired by the libraries dbplyr and ibis, the purpose of this document is decide on a testing approach, based on the strengths and weakenesses of these libraries.


## key decisions

* use dbplyr structure, 1 file for each verb (TBD, structure for e.g. vectors)
* based on ibis, include pandas as a backend, reference against raw pandas code
* ignore indexes, for certain backends, sort rows (since row order not garunteed)

---

## high level comparisons

* both test across a variety of backends
* dbplyr's file structure seems a bit more clear, since it focuses on the functions,
  rather than categories of things (e.g. ibis has an array file)
* I like that ibis includes their pandas backend in tests, all compared against raw pandas code

## dbplyr - three key components

The components below may exhibit backend specific behavior..

* sql expression api (i.e. no sqlalchemy)
* translators from R to sql
* verb S3 methods

## dbplyr - testing style

* verb tests
  - sql expression api
  - verb S3 methods compared to local (across DBs)
  - verify any attributes it puts on the LazyTbl
* backend tests
  - R -> sql translator behaviors specific to backends (e.g. `mean(x)` -> sql)
* sql expression api tests
  - not relevant to siuba, since it leans on sqlalchemy
  - exceptions for some key utilities, like window modifying tools

Other notes

* small dataframes are often created for tests / small sets of tests
* overall, I've found dbplyr's to be incredibly intuitive!

see 

* https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-distinct.R
* 

## ibis - 

* treats pandas as another backend (makes sense)
* a few central datasets used across tests
* lots of testing SQL query string construction, which we can skip?

see

* https://github.com/ibis-project/ibis/tree/master/ibis/tests
* https://github.com/ibis-project/ibis/blob/master/ibis/sql/postgres/tests/test_functions.py#L580
<!-- #endregion -->
