sqlmux
======

"Query multiplexing"

A library for merging multiple SQL queries to avoid the *N+1 Selects* problem[1].

This library was designed to work with autobatch[2].

From a list of SQL queries, those who are identical except in a single condition in the WHERE
clause, such as `WHERE x = 1`, `WHERE x = 2`, `WHERE x = 3`, are merged into a single query on the
form `WHERE x IN (1,2,3)`. The varying column (`x` in this case) is added to the SELECTed columns
so that the resulting rows can be divided and delivered to each of the original queries afterwards.

Currently a query has to be represented as a record `#sqlquery{}`. Perhaps it would be better
to use an existing representation of the SQL syntax tree such as a subset of ErlSQL[3] AKA Sqerl[4].

Note that dynamic SQL generation is generally considered bad practice.

* [1] http://stackoverflow.com/questions/97197/what-is-the-n1-selects-issue
* [2] https://github.com/zuiderkwast/autobatch
* [3] http://yarivsblog.blogspot.se/2006/09/introducing-erlsql-easy-expression-and.html
* [4] https://github.com/devinus/sqerl
