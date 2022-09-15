-module(qriak_pages).

%% EXTERNAL EXPORTS
-export([
    init/0,
    new_pages/1,
    process_pages/6
]).

%% TEST EXPORTS
-export([
    test/0
]).

%% TYPES
-type query_target() :: {bucket, binary()} | {keys, list()}.
-type pages_id() :: {bucket, binary()} | {query, binary()}.


%%-------------------------------------
%% EXTERNAL EXPORTS
%%-------------------------------------
-spec init() -> ok.
init() ->
    ets:new(?MODULE, [named_table, public]),
    ok.

-spec new_pages(QueryTarget :: query_target()) -> Resp :: binary().
new_pages({bucket, Bucket} = QueryTarget) ->
    PagesId = pages_id(),  
    case pages_exist(QueryTarget) of
        false ->
            {ok, IndexQuery} = 
                riak_index:to_index_query([
                    {field, "$bucket"},
                    {start_term, "key"},
                    {end_term, "key"}
                ]),
            {ok, Client} = riak:local_client(),
            {ok, BucketKeys} = riak_client:get_index(Bucket, IndexQuery, Client),
            SortedKeys = lists:sort(BucketKeys),
            SortedFullKeys = [{Bucket, K} || K <- SortedKeys],
            store_keys(QueryTarget, SortedFullKeys, 1),
            store_keys({query, PagesId}, SortedFullKeys, 1);
        true ->
            clone_pages(QueryTarget, {query, PagesId})
    end,
    PagesId;
new_pages({keys, FullKeys}) ->
    PagesId = pages_id(),
    SortedFullKeys = lists:sort(FullKeys),
    store_keys({query, PagesId}, SortedFullKeys, 1),
    PagesId.

-spec process_pages(PagesId, FromPos, Count, RedefineFun, MergeFun, ReadFun) -> Result when
    PagesId :: pages_id(),
    FromPos :: non_neg_integer(),
    Count :: pos_integer(),
    RedefineFun :: function(),
    MergeFun :: function(),
    ReadFun :: function(),
    Result :: any().
process_pages(PagesId, FromPos, Count, RedefineFun, MergeFun, ReadFun) ->
    [{{query, PagesId}, Keys, RedefinedCount}] = ets:lookup(?MODULE, {query, PagesId}),
    case (RedefinedCount < length(Keys)) and (RedefinedCount < (FromPos + Count)) of
        false ->
            io:format("Just read~n",[]),
            ToReadKeys = lists:sublist(Keys, FromPos, FromPos+Count-1),
            ReadFun(ToReadKeys);
        true ->
            io:format("Had to process~n",[]),
            RedefineFrom = min(FromPos, RedefinedCount),
            repeat_redefine_until(PagesId, Keys, RedefineFrom, FromPos, Count, RedefineFun, MergeFun, undefined)
    end.

repeat_redefine_until(PagesId, Keys, RedefineFrom, FromPos, Count, RedefineFun, MergeFun, PreviousResult) ->
    ToRedefineKeys = lists:sublist(Keys, RedefineFrom, FromPos+Count-1),
    RedefinedCount = length(ToRedefineKeys),
    Pre = do_pre(Keys, RedefineFrom),
    Post = do_post(Keys, RedefineFrom, RedefinedCount),
    {RedefinedKeys, Result} = RedefineFun(ToRedefineKeys),
    NewKeys = Pre ++ RedefinedKeys ++ Post,
    NewRedefineFrom = RedefineFrom + length(RedefinedKeys),
    store_keys({query, PagesId}, NewKeys, NewRedefineFrom),
    {NewResult, AccSize} = MergeFun(PreviousResult, Result),
    case (AccSize >= Count) or (length(Post) =:= 0) of
        true ->
            NewResult;
        false ->
            repeat_redefine_until(PagesId, NewKeys, NewRedefineFrom, NewRedefineFrom, Count, RedefineFun, MergeFun, NewResult)
    end.
    

%%-------------------------------------
%% INTERNAL EXPORTS
%%-------------------------------------
do_pre(Keys, FromPos) ->
    lists:sublist(Keys, 1, FromPos-1).

do_post(Keys, FromPos, Count) when FromPos + Count =< length(Keys)->
    lists:sublist(Keys, FromPos + Count, length(Keys));
do_post(_Keys, _FromPos, _Count) ->
    [].

pages_exist(QueryTarget) ->
    ets:member(?MODULE, QueryTarget).

store_keys(QueryTarget, Keys, RedefinedCount) ->
    true = ets:insert(?MODULE, {QueryTarget, Keys, RedefinedCount}),
    ok.

clone_pages(PagesId1, PagesId2) ->
    [{PagesId1, PagesEntry}] = ets:lookup(?MODULE, PagesId1),
    ets:insert(?MODULE, {PagesId2, PagesEntry}).

pages_id() ->
    list_to_binary(riak_core_util:unique_id_62()).


%%-------------------------------------
%% TEST EXPORTS
%%-------------------------------------  
test() ->
    init(),
    % Testing how get_pages and redefine_pages work
    AllPages1 = 
        [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,
         19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,
         34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50],
    PagesId1 = new_pages({keys, AllPages1}),

    RedefineFun1 =
        fun(KeyList) ->
            OddsNo5MultipleFun = 
                fun
                    (N) when N rem 2 /= 0, N rem 5 /= 0 -> {true, {N, integer_to_list(N)}};
                    (_) -> false 
                end,
            Out1 = lists:filtermap(OddsNo5MultipleFun, KeyList),
            lists:unzip(Out1)
        end,
    
    ReadFun1 =
        fun(KeyList) ->
            lists:map(fun(X) -> integer_to_list(X) end, KeyList)
        end,

    MergeFun1 =
        fun
            (undefined, Resp2) -> {Resp2, length(Resp2)};
            (Resp1, Resp2) -> NewResp = Resp1 ++ Resp2, {NewResp, length(NewResp)}
        end,

    ["1","3","7","9","11","13","17","19","21","23"] = lists:sublist(process_pages(PagesId1, 1, 10, RedefineFun1, MergeFun1, ReadFun1),1,10),
    ["1","3","7","9","11","13","17","19","21","23"] = lists:sublist(process_pages(PagesId1, 1, 10, RedefineFun1, MergeFun1, ReadFun1),1,10),
    ["11","13","17","19","21","23","27","29", "31", "33"] = lists:sublist(process_pages(PagesId1, 5, 10, RedefineFun1, MergeFun1, ReadFun1),1,10),
    AllProcessed = [
        "1","3","7","9","11","13","17","19","21","23","27",
        "29","31","33","37","39","41","43","47","49"],
    AllProcessed = process_pages(PagesId1, 1, 100, RedefineFun1, MergeFun1, ReadFun1).
     