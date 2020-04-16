User API
============


.. module:: siuba.dply.verbs

.. toctree::
    :maxdepth: 2
    :caption: Core One-table verbs
    :hidden:
    :glob:

    api_table_core/*
  
.. toctree::
    :maxdepth: 2
    :caption: Other One-table Verbs
    :hidden:
    :glob:

    api_table_other/*
  
.. toctree::
    :maxdepth: 2
    :caption: Two-table Verbs
    :hidden:
    :glob:

    api_table_two/*

.. toctree::
    :maxdepth: 2
    :caption: Tidy Verbs
    :hidden:
    :glob:

    api_tidy/*

.. rubric:: Core one-table verbs          

.. table::
   :class: table-align-left

   ===========================================  =======================================================
    :ref:`Filter`                                |filter|
    :ref:`Arrange`                               |arrange|
    :ref:`Select`, :ref:`Rename`                 |select|
    :ref:`Mutate`, :ref:`Transmute`              |mutate|
    :ref:`Summarize`                             |summarize|
    :ref:`Group by`                              |group_by|
   ===========================================  =======================================================

.. |filter| replace:: Keep rows that match condition.
.. |arrange| replace:: Sort rows based on one or more columns.
.. |select| replace:: Keep or rename specific columns.
.. |mutate| replace:: Create or replace a column.
.. |summarize| replace:: Calculate a single number per grouping.
.. |group_by| replace:: Specify groups for splitting rows of the data.



.. rubric:: Other one-table verbs          

.. table::
   :class: table-align-left

   ===========================================  =======================================================
    :ref:`Distinct`                              |distinct|
    :ref:`Count`                                 |count|
   ===========================================  =======================================================

.. |distinct| replace:: Keep only rows with unique values.
.. |count| replace:: Group by columns and count the number of observations.

.. rubric:: Two-table verbs

.. table::
   :class: table-align-left

   ===========================================  =======================================================
    :ref:`Joins`                                 |joins|
   ===========================================  =======================================================

.. |joins| replace:: Merge two tables using specific columns.


.. rubric:: Tidy verbs

.. table::
   :class: table-align-left

   ===========================================  =======================================================
    :ref:`Nest`                                  |nest|
    :ref:`Gather`, :ref:`Spread`                 |gather|
   ===========================================  =======================================================

.. |nest| replace:: Create a column where each row is a DataFrame.
.. |gather| replace:: Reshape data between long and wide formats.

