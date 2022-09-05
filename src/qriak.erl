-module(qriak).

%% INCLUDES
-include("qriak.hrl").

-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("eunit/include/eunit.hrl").

%% EXTERNAL EXPORTS
-export([select/1, select/2]).

%% SPECS
-spec select(Query) -> Resp when
  Query :: string(),
  Resp :: qriak_response().

-spec select(Bucket, Conditions) -> Resp when
  Bucket :: binary(),
  Conditions :: qriak_conditions(),
  Resp :: qriak_response().

%%-------------------------------------
%% EXTERNAL EXPORTS
%%-------------------------------------
select(Query) when is_list(Query) ->
  try
    {ok, Tokens, _} = qriak_lexer:string(Query),
    QueryTerm = qriak_parser:parse(Tokens),
    run_query(QueryTerm)
  catch
    _:Error ->
      {error, Error}
  end.
  

select(Bucket, Conditions) ->
  {ok, Client} = riak:local_client(),
  Keys = evalue_conditions_2i(Client, Bucket, Conditions, is_or),
  ActualBucket = 
    case Keys of
      no_relevant ->
        Bucket;
      [] ->
        dont_try;
      _KeyList ->
        [{Bucket, K} || K <- Keys]
    end,
    

  MapFun = fun(O, _, _) ->
    Metadata = riak_object:get_metadata(O),
    Indexes = riakc_obj:get_secondary_indexes(Metadata),
    EvalOut = evalue_conditions(Indexes, Conditions, is_or),
    Eval = case EvalOut of
      false -> false;
      true -> true;
      no_relevant -> true;
      _ -> false
    end,
    case Eval of
      true -> 
        ResponseFields = 
          lists:filtermap(
            fun
                ({{binary_index, IndexKey}, IndexVal}) -> {true, {IndexKey, IndexVal}};
                (_) -> false
            end,
            Indexes),
        Info = 
          qriak_respinfo:total_count(
            qriak_respinfo:new()),
        [{match, {response_info, Info} , [ResponseFields]}];
      false -> 
        [no_match]
    end
  end,

  ReduceFun = fun(Responses, _Arg) ->
    [lists:foldl(fun(Response, {match,{response_info, MergedInfo}, AcuFields} = Acc) ->
        case Response of
          no_match ->
            Acc;
          {match, {response_info, ResponseInfo}, [ [{_K, _V}|_] = RespFields] } ->
            case AcuFields of
              [] ->
                {match, {response_info, qriak_respinfo:merge(ResponseInfo, MergedInfo)}, [ RespFields ]};
              List ->
                {match, {response_info, qriak_respinfo:merge(ResponseInfo, MergedInfo)}, [ RespFields | List]}
            end;
          % ReReduce response
          {match, {response_info, RRResponseInfo}, RRList} ->
            case AcuFields of
              [] ->
                {match, {response_info, qriak_respinfo:merge(RRResponseInfo, MergedInfo)}, RRList};
              [RRAcuField] ->
                {match, {response_info,qriak_respinfo:merge(RRResponseInfo, MergedInfo)}, [RRAcuField | RRList]}
            end                        
        end
      end,
      {match, {response_info, qriak_respinfo:new()}, []},
      Responses) ]
    end,

  MapRedFunTuples =
    [{map, {qfun, MapFun}, [], false},
     {reduce, {qfun, ReduceFun}, [], true}],


  Output =
    case ActualBucket of
      dont_try ->
        {ok, []};
      Bucket when is_binary(Bucket) ->   
        riak_kv_mrc_pipe:mapred(Bucket,
          MapRedFunTuples);
      KeyList when is_list(KeyList) ->
        riak_kv_mrc_pipe:mapred(KeyList,
          MapRedFunTuples)
    end,

  case Output of
    {ok, []} ->
      #qriak_response{
        info = #{total_count => 0},
        items = []};
    {ok, [{match, {response_info, Info}, Items}]} ->
      #qriak_response{
        info = Info,
        items = Items};
    Other ->
      Other
  end.

%%-------------------------------------
%% INTERNAL FUNCTIONS
%%-------------------------------------
run_query({ok, QueryTerm}) ->
  case qriak_dsl2api:convert(QueryTerm) of
    {Bucket, Conditions} ->
      select(Bucket, Conditions);
    _ ->
      bad_format_query
  end;
run_query(_) ->
  {error, bad_format_query}.

evalue_conditions(Indexes, Conditions, is_or) ->
  lists:foldl(
    fun(Evaluation, AccIn) -> 
      case Evaluation of
        no_relevant -> AccIn;
        _ -> Evaluation or AccIn
      end
    end,
    false,
    [evalue_condition(Indexes, C) || C <- Conditions]);
evalue_conditions(Indexes, Conditions, is_and) ->
  lists:foldl(
    fun(Evaluation, AccIn) -> 
      case Evaluation of
        no_relevant -> AccIn;
        _ -> Evaluation and AccIn
      end
    end,
    true,
    [evalue_condition(Indexes, C) || C <- Conditions]).

evalue_condition(Indexes, {Field, reads_like, Value}) ->
  reads_like(proplists:lookup({binary_index, Field}, Indexes), Value);
evalue_condition(Indexes, {Field, colour_like, Value}) ->
  colour_like(proplists:lookup({binary_index, Field}, Indexes), Value);
evalue_condition(_Indexes, {_Field, is, _Value}) ->
  true;
evalue_condition(_Indexes, {_Field, in_range, _Values}) ->
  true;      
evalue_condition(Indexes, {is_and, Conditions}) ->
  evalue_conditions(Indexes, Conditions, is_and);
evalue_condition(Indexes, {is_or, Conditions}) ->
  evalue_conditions(Indexes, Conditions, is_or);
evalue_condition(_Indexes, _Condition) ->
  no_relevant.


evalue_conditions_2i(Client, Bucket, Conditions, is_or) ->
  lists:foldl(
    fun(KeyList, AccIn) -> 
      case KeyList of
        no_relevant ->
          case AccIn of
            no_relevant -> no_relevant;
            _ -> AccIn
          end;
        _List ->
          list_union(KeyList, AccIn)
      end
    end,
    no_relevant,
    [evalue_condition_2i(Client, Bucket, C) || C <- Conditions]);
evalue_conditions_2i(Client, Bucket, Conditions, is_and) ->
  lists:foldl(
    fun(KeyList, AccIn) ->
      case KeyList of
        no_relevant ->
          case AccIn of
            no_relevant -> no_relevant;
            _ -> AccIn
          end;
        _List ->
          list_intersect(KeyList, AccIn)
      end
    end,
    no_relevant,
    [evalue_condition_2i(Client, Bucket, C) || C <- Conditions]).

evalue_condition_2i(Client, Bucket, {Field, is, BinValue}) ->
  {ok, KeyList} = query_index(Client, Bucket, Field, BinValue),
  KeyList;
evalue_condition_2i(Client, Bucket, {Field, in_range, {BinVal1, BinVal2}}) ->
  {ok, KeyList} = query_index(Client, Bucket, Field, BinVal1, BinVal2),
  KeyList;  
evalue_condition_2i(Client, Bucket, {is_and, Conditions}) ->
  evalue_conditions_2i(Client, Bucket, Conditions, is_and);
evalue_condition_2i(Client, Bucket, {is_or, Conditions}) ->
  evalue_conditions_2i(Client, Bucket, Conditions, is_or);
evalue_condition_2i(_Client, _Bucket, _Conditions) ->
  no_relevant.

reads_like(_Str1, _Str2) ->
  true.

colour_like(_Col1, _Col2) ->
  true.

query_index(Client, Bucket, Field, BinValue) ->
  Field2i = field_to_index_key(Field),
  {ok, IndexQuery} = 
    riak_index:to_index_query([
      {field, Field2i}, 
      {start_term, BinValue}, 
      {end_term, BinValue}, 
      {term_regex, undefined},
      {max_results, undefined}, 
      {return_terms, false}, 
      {continuation, undefined}]),
  riak_client:get_index(
    Bucket, 
    IndexQuery, 
    Client).

query_index(Client, Bucket, Field, BinValue1, BinValue2) ->
  {ok, IndexQuery} =
    riak_index:to_index_query([
      {field, field_to_index_key(Field)}, 
      {start_term, BinValue1}, 
      {end_term, BinValue2}, 
      {term_regex, undefined},
      {max_results, undefined}, 
      {return_terms, false}, 
      {continuation, undefined}]),
  riak_client:get_index(
    Bucket, 
    IndexQuery, 
    Client).

field_to_index_key(Field) when is_list(Field) ->
  << (list_to_binary(Field))/binary, "_bin" >>;
field_to_index_key(Field) when is_binary(Field)->
  << Field/binary, "_bin" >>.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% UTIL FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
list_intersect(List, no_relevant) ->
  List;
list_intersect(List1, List2) ->
  [L1 || L1 <- List1, L2 <- List2, L1 =:= L2].

list_union(List, no_relevant) ->
  List;
list_union(List1, List2) ->
  lists:usort(List1 ++ List2).
