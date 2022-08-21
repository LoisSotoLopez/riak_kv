-module(qriak_respinfo).

% MERGE EXPORTS
-export([
    merge/2
]).
% RESPONSE INFO PIECES EXPORTS
-export([
    new/0,
    total_count/1,
    total_count/2,
    merge_with/3,
    total_count_mergefun/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SPECS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-type respinfo() :: map().
% Need to spec same type
-type combiner(Type) :: fun((atom(), Type, Type) -> Type).

-spec new() -> respinfo().
-spec total_count(respinfo()) -> respinfo().
-spec total_count(respinfo(), integer()) -> respinfo().
-spec merge(respinfo(), respinfo()) -> respinfo().
-spec merge_with(combiner(any()), respinfo(), respinfo()) -> respinfo().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% MERGE EXPORTS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
merge(RespInfo1, RespInfo2) ->
    Combiner =
        fun(Key, Value1, Value2) ->
            MergeFunName = list_to_atom(atom_to_list(Key) ++ "_mergefun"),
            erlang:apply(?MODULE, MergeFunName, [Value1, Value2])
        end,
    qriak_respinfo:merge_with(
        Combiner,
        RespInfo1,
        RespInfo2).

total_count_mergefun(Value1, Value2) ->
    Value1 + Value2.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RESPONSE INFO PIECES EXPORTS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
new() ->
    maps:new().

total_count(RespInfo) ->
    total_count(RespInfo, 1).

total_count(RespInfo, N) ->
    maps:put(total_count, N, RespInfo).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% UTIL FUNCTIONS EXPORTS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

merge_with(Combiner, Map1, Map2) when is_map(Map1),
                                    is_map(Map2),
                                    is_function(Combiner, 3) ->
    case erlang:map_size(Map1) > erlang:map_size(Map2) of
        true ->
            Iterator = maps:iterator(Map2),
            merge_with_1(maps:next(Iterator),
                Map1,
                Map2,
                Combiner);
        false ->
            Iterator = maps:iterator(Map1),
            merge_with_1(maps:next(Iterator),
                Map2,
                Map1,
                fun(K, V1, V2) -> Combiner(K, V2, V1) end)
    end;
merge_with(Combiner, Map1, Map2) ->
    error_with_info(error_type_merge_intersect(Map1, Map2, Combiner),
    [Combiner, Map1, Map2]).

merge_with_1({K, V2, Iterator}, Map1, Map2, Combiner) ->
    case Map1 of
        #{ K := V1 } ->
            NewMap1 = Map1#{ K := Combiner(K, V1, V2) },
            merge_with_1(maps:next(Iterator), NewMap1, Map2, Combiner);
        #{ } ->
            merge_with_1(maps:next(Iterator), maps:put(K, V2, Map1), Map2, Combiner)
    end;
    merge_with_1(none, Result, _, _) ->
    Result.

error_type_two_maps(M1, M2) when is_map(M1) ->
    {badmap, M2};
error_type_two_maps(M1, _M2) ->
    {badmap, M1}.

error_type_merge_intersect(M1, M2, Combiner) when is_function(Combiner, 3) ->
    error_type_two_maps(M1, M2);
error_type_merge_intersect(_M1, _M2, _Combiner) ->
    badarg.

error_with_info(Reason, Args) ->
    erlang:error({Reason, Args}).
