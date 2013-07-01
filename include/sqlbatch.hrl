%% @doc A 'where condition' is either a pair on the form {Column, Value} corresponding to an equals
%% condition or a string/binary/iolist of SQL code (an SQL expression). In the former, Value is an
%% unquoted value.
-type where_cond() :: {term(), term()} | iolist().

%% @doc An SQL SELECT query. See make_select/4 for documentation.
-record(sqlquery, {select      :: [iolist()],
                   from   = [] :: iolist(),
                   where  = [] :: where_cond(),
                   other  = [] :: iolist()}).
