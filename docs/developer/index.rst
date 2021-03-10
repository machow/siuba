Developer docs
==============

.. toctree::
    :maxdepth: 2
    :hidden:

    backend_sql.Rmd
    backend_pandas.rst
    call_trees.Rmd
    sql-translators.ipynb
    pandas-group-ops.Rmd

.. rubric:: Backends

.. table::
   :class: table-align-left

   ===========================================  =======================================================
    :doc:`Sql Backend <backend_sql>`             |sqlbackend|
    :doc:`Pandas Backend <backend_pandas>`       |pandasbackend|
   ===========================================  =======================================================


.. |sqlbackend|    replace:: How to implement translations for a new SQL dialect, or extend the SQL backend.
.. |pandasbackend| replace:: How the pandas backend implements fast grouped operations.



.. rubric:: Core

.. table::
   :class: table-align-left

   ========================================================  =======================================================
    :doc:`Sql Translators <sql-translators>`                  |translators|
    :doc:`Call Trees <call_trees>`                            |calls|
    :doc:`Fast Pandas Grouped Ops <pandas-group-ops>`         |fastgrouped|
   ========================================================  =======================================================

.. |calls|         replace:: Calls represent "what" operations users want to do. This document describes how they are constructed, transformed, and executed.
.. |translators|   replace:: A closer look at the translation process, focused on SQL and the CallTreeLocal tree listener.
.. |fastgrouped|   replace:: Why are grouped operations cumbersome? How does siuba simplify them?


