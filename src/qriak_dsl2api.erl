-module(qriak_dsl2api).

%% External exports
-export([convert/1]).

%%-------------------------------------
%% EXTERNAL EXPORTS
%%-------------------------------------

convert(QueryTerms) when is_list(QueryTerms)->
    Bucket = proplists:get_value(tables, QueryTerms),
    DslConditions = proplists:get_value(where, QueryTerms),
    ConversionFun = 
      fun (DslCondition) ->
        convert_condition(DslCondition)
      end,
    ApiConditions = lists:map(ConversionFun, DslConditions),
    {Bucket, ApiConditions}.


%%-------------------------------------
%% INTERNAL EXPORTS
%%-------------------------------------
convert_condition({is_, Field, Value}) ->
    {binary_to_list(Field), is, Value};
convert_condition({between, Field, Value1, Value2}) ->
    {binary_to_list(Field), in_range, {Value1, Value2}};
convert_condition({reads_like, Field, Value}) ->
    {binary_to_list(Field), reads_like, Value};
convert_condition({colour_like, Field, Value}) ->
    {binary_to_list(Field), colour_like, Value};
convert_condition({and_, Cond1, Cond2}) ->
    {is_and, [convert_condition(Cond1), convert_condition(Cond2)]};
convert_condition({or_, Cond1, Cond2}) ->
    {is_or, [convert_condition(Cond1), convert_condition(Cond2)]}.
