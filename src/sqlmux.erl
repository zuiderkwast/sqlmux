%% @doc A module for merging multiple similar SQL queries to overcome the Select N+1 Problem.
%%
%% Can merge multiple queries on the form "... WHERE x = 1", "... WHERE x = 2", "... WHERE x = 3"
%% into a single query on the form "... WHERE x IN (1,2,3)".
%%
%% Note that dynamic SQL generation is often considered bad practice, especially when done in an
%% uncontrolled manner.
-module(sqlmux).

-export([create_query/3, create_query/4, sqlquery_to_iolist/2,
         query_all/3, query_some/3]).
-export_type([query_fun/0, query_id/0, escape_fun/0, sqlquery/0, where_cond/0]).

%% @doc Any function that takes an SQL query in the form of an iolist(), sends it to a database and
%% returns the rows in the form of a nested list of values.
-type query_fun() :: fun((iolist()) -> [[term()]]).

-type query_id() :: term().

%% @doc A function that takes a term and returns a string/binary/iolist containing an SQL
%% expression. E.g. for strings, quotes should be added and special characters escaped.
-type escape_fun() :: fun((term()) -> iolist()).

-include("sqlmux.hrl").

-type sqlquery() :: #sqlquery{}.

%% @doc Constructs a sqlquery(). See create_query/4.
-spec create_query(Select::[term()], From::iolist(), Where::[where_cond()]) -> sqlquery().
create_query(Select, From, Where) ->
	#sqlquery{select = Select, from = From, where = Where}.

%% @doc Constructs a sqlquery(). If you include sqlmux.hrl, you can also use the record
%% #sqlquery{} directly.
%%
%% Select is a list of columns to select. These should be strings/binaries/iolists without any
%% characters that need to be escaped in SQL.
%%
%% From is the table name as a string/binary/iolist.
%%
%% Where is a list of where conditions. Each condition is either a string/binary/iolist of SQL
%% code (an SQL expression) or a pair on the form {Column, Value} corresponding to an equality
%% condition "Column = Value". In the latter, Value is an unquoted value.
%%
%% Other may be anything after the WHERE clause, s.t. ORDER BY or FOR UPDATE, as SQL code.
-spec create_query(Select::[term()], From::iolist(), Where::[where_cond()], Other::iolist()) ->
	sqlquery().
create_query(Select, From, Where, Other) ->
	#sqlquery{select = Select, from = From, where = Where, other = Other}.

%% @doc Returns an iolist() contining the SQL code for a sqlquery() structure.
-spec sqlquery_to_iolist(escape_fun(), sqlquery()) -> iolist().
sqlquery_to_iolist(QuoteFun, #sqlquery{select=Select, from=From, where=Where, other=Other}) ->
	SelectPart = [<<"SELECT ">>, join(<<",">>, Select)],
	FromPart  = case From  of [] -> []; _ -> [<<" FROM ">>, From] end,
	WherePart = case Where of
		[] -> [];
		_  ->
			CondQuoter = fun ({Key, Value}) -> [Key, <<"=">>, QuoteFun(Value)];
			                 (Cond) when is_list(Cond); is_binary(Cond) -> Cond
		                     end,
			[<<" WHERE ">>, join(<<" AND ">>, lists:map(CondQuoter, Where))]
	end,
	OtherPart = case Other of [] -> []; _ -> [<<" ">>, Other] end,
	[SelectPart, FromPart, WherePart, OtherPart].

%% @doc Returns the results for each query in the list. Though the queries appear to be executed
%% one by one, similar queries are merged into batches when possible.
-spec query_all(query_fun(), escape_fun(), [{query_id(), sqlquery()}]) -> [[[term()]]].
query_all(QueryFun, QuoteFun, QueryList) ->
	query_all(QueryFun, QuoteFun, QueryList, []).
query_all(_QueryFun, _QuoteFun, [], Acc) ->
	lists:flatten(lists:reverse(Acc));
query_all(QueryFun, QuoteFun, QueryList, Acc) ->
	SomeAnswers = query_some(QueryFun, QuoteFun, QueryList),
	QueriesLeft = lists:filter(fun ({Qid, _}) -> not lists:keymember(Qid, 1, SomeAnswers) end,
	                           QueryList),
	query_all(QueryFun, QuoteFun, QueriesLeft, [SomeAnswers | Acc]).

%% @doc attempts to combine some or all of the queries into one, performs it using QueryFun and
%% divides the results per query. Only queries that are similar enough to be merged into one are
%% executed. At least one query will be answered. QueryFun should take an SQL query and return the
%% rows.
-spec query_some(query_fun(), escape_fun(), [{query_id(), Query :: sqlquery()}, ...]) ->
	[{query_id(), Rows :: [[term()]]}].
query_some(QueryFun, QuoteFun, QueryList) ->
	{Query, VaryCol, VaryValues, Qids} = merge_some(QueryList),
	case VaryValues of
		[] ->
			%% There is no varying col. Perform the query and return the same for all.
			Sql = sqlquery_to_iolist(QuoteFun, Query),
			Rows = QueryFun(Sql),
			[{Qid, Rows} || Qid <- Qids];
		_ ->
			%% There is a varying col. Prepend it to the query.
			Select = [VaryCol|Query#sqlquery.select],
			%% Add an IN (...) to the WHERE clause
			In = [VaryCol, <<" IN (">>, join(<<",">>, lists:map(QuoteFun, VaryValues)),
			      <<")">>],
			Where = [In|Query#sqlquery.where],
			Query1 = Query#sqlquery{select = Select, where  = Where},
			Sql = sqlquery_to_iolist(QuoteFun, Query1),
			Rows = QueryFun(Sql),
			%% Extract the first value in each row and divide the rows on the answered
			%% queries.
			VaryValuesAndRows = [{X, Xs} || [X|Xs] <- Rows],
			AnsweredQueries = lists:filter(fun({Qid, _}) -> lists:member(Qid, Qids) end,
			                               QueryList),
			distribute_rows(VaryCol, VaryValuesAndRows, AnsweredQueries)
	end.

%% @doc Merges multiple SQL queries differing only in one condition in the WHERE clauses on the
%% form "column = value1", "column = value2", ... into a single query with this condition merged
%% into a condition on the form "column IN (value1, value2, ...)", ignoring those who don't match.
%%
%% VaryCol is the name of the varying column or 'none' if no column is varying.
-spec merge_some([{query_id(), Query :: sqlquery()}, ...]) ->
	{CommonQuery :: sqlquery(), VaryCol::term(), VaryValues::[term()], [query_id(), ...]}.
merge_some([{Id, Query}|Queries]) ->
	merge_some(Queries, Query, none, [], [Id]).

merge_some([{Id, Query}|Queries], Batch, none, [], Ids) ->
	case query_extract_common(Query, Batch) of
		{ok, Batch1, WhereCol, WhereValues} ->
			merge_some(Queries, Batch1, WhereCol, WhereValues, [Id|Ids]);
		nomatch ->
			merge_some(Queries, Batch, none, [], Ids)
	end;
merge_some([{Id, Query}|Queries], Batch, WhereCol, WhereValues, Ids) ->
	case query_match_batch(Query, Batch, WhereCol, WhereValues) of
		{ok, WhereValues1} -> merge_some(Queries, Batch, WhereCol, WhereValues1, [Id|Ids]);
		nomatch            -> merge_some(Queries, Batch, WhereCol, WhereValues, Ids)
	end;
merge_some([], Batch, WhereCol, WhereValues, Ids) ->
	{Batch, WhereCol, lists:reverse(WhereValues), lists:reverse(Ids)}.

%% @doc Divedes the results of a merged "col IN (...)" query onto different "col = ..." queries. The
%% queries are assumed to match, apart from the varying column VaryCol which is checked against the
%% VaryValue for each row and query. For each query, a list of matching rows are returned.
-spec distribute_rows(VaryCol :: term(),
                      [{VaryValue :: term(), Row::[term()]}],
                      [{query_id(), sqlquery()}]) -> [{query_id(), Rows::[[term()]]}].
distribute_rows(_VaryCol, _VaryingValuesAndRows, []) -> [];
distribute_rows(VaryCol, VaryingValuesAndRows, [{Qid, #sqlquery{where = Where}}|QueryList]) ->
	Rows = [Row || {VaryValue, Row} <- VaryingValuesAndRows,
	               lists:member({VaryCol, VaryValue}, Where)],
	[{Qid, Rows}|distribute_rows(VaryCol, VaryingValuesAndRows, QueryList)].

%% @doc Finds the single differing equality condition in WHERE clause of two queries.
%% Returns a tuple tagged 'ok' with the common query (with the differing condition removed), a
%% column name and a list of the different values of that column.
%% If Values is an empty list, the queries are identical. If the queries differ in more than an
%% equality condition, 'nomatch' is returned.
-spec query_extract_common(sqlquery(), sqlquery()) -> {ok, sqlquery(), Col::term(), Values::list()} | nomatch.
query_extract_common(Q, Q) ->
	{ok, Q, none, []};
query_extract_common(#sqlquery{select = S, from = F, where = W1, other = O},
                   #sqlquery{select = S, from = F, where = W2, other = O}) ->
	case where_extract_common(W1, W2) of
		{ok, Where, Col, Values} ->
			Q = #sqlquery{select = S, from = F, where = Where, other = O},
			{ok, Q, Col, Values};
		nomatch ->
			nomatch
	end;
query_extract_common(_, _) ->
	nomatch.

%% @doc Extracts the single differing condition in two lists of conditions, if possible.
%% Values is an empty list if the lists of conditions are identical.
-spec where_extract_common(list(), list()) -> {ok, list(), Col::term(), Values::list()} | nomatch.
where_extract_common(W1, W2) ->
	where_extract_common(W1, W2, []).

where_extract_common([], [], CommonAcc) ->
	{ok, lists:reverse(CommonAcc), none, []};
where_extract_common([Cond|Conds1], [Cond|Conds2], CommonAcc) ->
	where_extract_common(Conds1, Conds2, [Cond|CommonAcc]);
where_extract_common([{Col,V1}|Conds], [{Col,V2}|Conds], CommonAcc) ->
	{ok, lists:reverse(CommonAcc) ++ Conds, Col, [V1, V2]};
where_extract_common(_, _, _) ->
	nomatch.

%% @doc Extracts the condition on column WhereCol from a query which otherwise matches another
%% query (Batch), if possible.
-spec query_match_batch(sqlquery(), sqlquery(), term(), list()) -> {ok, list()} | nomatch.
query_match_batch(#sqlquery{select = S, from = F, where = W1, other = O},
                  #sqlquery{select = S, from = F, where = W2, other = O},
                  WhereCol, WhereValues) ->
	where_match_batch(W1, W2, WhereCol, WhereValues);
query_match_batch(_, _, _, _) ->
	nomatch.

%% @doc Extracts the condition on column WhereCol from a list which otherwise matches another
%% list of conditions, if possible.
-spec where_match_batch(list(), list(), term(), list()) -> {ok, list()} | nomatch.
where_match_batch([], [], _, WhereValues) ->
	{ok, WhereValues};
where_match_batch([{WhereCol, Value}|Conds], Conds, WhereCol, WhereValues) ->
	WhereValues1 = case lists:member(Value, WhereValues) of
		true  -> WhereValues;
		false -> [Value|WhereValues]
	end,
	{ok, WhereValues1};
where_match_batch([Cond|Conds1], [Cond|Conds2], WhereCol, WhereValues) ->
	where_match_batch(Conds1, Conds2, WhereCol, WhereValues);
where_match_batch(_, _, _, _) ->
	nomatch.

%% @doc Concatenates a list of iolist()s into a single iolist(), with a separator inserted between
%% every pair of elements.
-spec join(iolist(), [iolist()]) -> iolist().
join(_Sep, [X])   -> [X];
join(Sep, [X|Xs]) -> [X, Sep | join(Sep, Xs)].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

my_query(Query) ->
	case list_to_binary(Query) of
		<<"SELECT bar,a FROM foo WHERE bar IN ('baz','jar') AND foo=42">> ->
			[["baz", 2], ["jar", 333]];
		<<"SELECT c FROM foo WHERE foo=42 AND bar='jar'">> ->
			[[999]]
	end.

my_quote(X) when is_integer(X)            -> integer_to_list(X);
my_quote(X) when is_binary(X); is_list(X) -> [<<"'">>, X, <<"'">>].

sqlquery_to_iolist_test() ->
	Q = #sqlquery{select = ["a", "b", "c"],
	            from   = "foo",
	            where  = ["a > 0", {"b", 5}],
	            other  = "ORDER BY c"},
	Sql = sqlquery_to_iolist(fun my_quote/1, Q),
	<<"SELECT a,b,c FROM foo WHERE a > 0 AND b=5 ORDER BY c">> = list_to_binary(Sql).

merge_some_test() ->
	Q1 = #sqlquery{select = ["a"], from = "foo", where = [{"foo", 42}, {"bar", "baz"}]},
	Q2 = #sqlquery{select = ["a"], from = "foo", where = [{"foo", 42}, {"bar", "jar"}]},
	{Q3, "bar", ["baz", "jar"], [1, 2]} = merge_some([{1, Q1}, {2, Q2}]),
	Q3 = #sqlquery{select = ["a"], from = "foo", where = [{"foo", 42}]}.

query_some_test() ->
	Q1 = #sqlquery{select = ["a"], from = "foo", where = [{"foo", 42}, {"bar", "baz"}]},
	Q2 = #sqlquery{select = ["a"], from = "foo", where = [{"foo", 42}, {"bar", "jar"}]},
	Q3 = #sqlquery{select = ["b"], from = "foo", where = [{"foo", 42}, {"bar", "jar"}]},
	Queries = [{1, Q1}, {2, Q2}, {3, Q3}],
	Result = query_some(fun my_query/1, fun my_quote/1, Queries),
	[{1, [[2]]}, {2, [[333]]}] = Result.

query_all_test() ->
	Q1 = #sqlquery{select = ["a"], from = "foo", where = [{"foo", 42}, {"bar", "baz"}]},
	Q2 = #sqlquery{select = ["a"], from = "foo", where = [{"foo", 42}, {"bar", "jar"}]},
	Q3 = #sqlquery{select = ["c"], from = "foo", where = [{"foo", 42}, {"bar", "jar"}]},
	Queries = [{1, Q1}, {2, Q2}, {3, Q3}],
	Result = query_all(fun my_query/1, fun my_quote/1, Queries),
	[{1, [[2]]}, {2, [[333]]}, {3, [[999]]}] = Result.

-endif.
