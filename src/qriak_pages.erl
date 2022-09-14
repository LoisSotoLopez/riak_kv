-module(qriak_pages).

%% EXTERNAL EXPORTS
-export([
    init/0,
    new_pages/1,
    get_pages/3,
    redefine_pages/4
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
            store_keys(QueryTarget, SortedFullKeys),
            store_keys({query, PagesId}, SortedFullKeys);
        true ->
            clone_pages(QueryTarget, {query, PagesId})
    end,
    PagesId;
new_pages({keys, FullKeys}) ->
    PagesId = pages_id(),
    SortedFullKeys = lists:sort(FullKeys),
    store_keys({query, PagesId}, SortedFullKeys),
    PagesId.
    
-spec get_pages(PagesId, FromPos, Count) -> Resp when
    PagesId :: pages_id(),
    FromPos :: non_neg_integer(),
    Count :: pos_integer(),
    Resp :: list().
get_pages(PagesId, FromPos, Count) ->
    [{{query, PagesId}, Keys}] = ets:lookup(?MODULE, {query, PagesId}),
    lists:sublist(Keys, FromPos, Count).

-spec redefine_pages(PagesId, FromPos, Count, NewKeys) -> Resp when
    PagesId :: pages_id(),
    FromPos :: non_neg_integer(),
    Count :: pos_integer(),
    NewKeys :: list(),
    Resp :: ok.
redefine_pages(PagesId, FromPos, Count, RedefinedKeys) ->
    [{{query, PagesId}, Keys}] = ets:lookup(?MODULE, {query, PagesId}),
    Pre = do_pre(Keys, FromPos),
    Post = do_post(Keys, FromPos, Count),
    NewKeys = Pre ++ RedefinedKeys ++ Post,
    store_keys({query, PagesId}, NewKeys).

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

store_keys(QueryTarget, Keys) ->
    true = ets:insert(?MODULE, {QueryTarget, Keys}),
    ok.

clone_pages(PagesId1, PagesId2) ->
    [{PagesId1, Pages}] = ets:lookup(?MODULE, PagesId1),
    ets:insert(?MODULE, PagesId2, Pages).

pages_id() ->
    list_to_binary(riak_core_util:unique_id_62()).


%%-------------------------------------
%% TEST EXPORTS
%%-------------------------------------  
test() ->
    init(),
    % Testing how get_pages and redefine_pages work
    PageId1 = new_pages({keys, [1,2,3,4,5,6,7,8,9,10]}),
    PageSize = 4,

    ActualPage0 = 1,
    [1,2,3,4] = get_pages(PageId1, ActualPage0, PageSize),
    MapRed1Out = [1,2,4],
    redefine_pages(PageId1, ActualPage0, PageSize, MapRed1Out),
    [1,2,4,5] = get_pages(PageId1, ActualPage0, PageSize),

    ActualPage1 = ActualPage0 + PageSize,
    [6,7,8,9] = get_pages(PageId1, ActualPage1, PageSize),
    MapRed2Out = [6,7,8,9],
    redefine_pages(PageId1, ActualPage1, PageSize, MapRed2Out),
    [6,7,8,9] = get_pages(PageId1, ActualPage1, PageSize),
    
    
    ActualPage2 = ActualPage1 + PageSize,
    [10] = get_pages(PageId1, ActualPage2, PageSize),
    MapRed3Out = [],
    redefine_pages(PageId1, ActualPage2, PageSize, MapRed3Out),
    [] = get_pages(PageId1, ActualPage2, PageSize),

    % Testing how to be used by qriak.erl
    PagesId2 = new_pages({keys, [
        {true,1},{true,2},{false,3},{true,4},{true,5},
        {false,6},{false,7},{true,8},{true,9},{true,10}]}),
    MapRedFun =
        fun
            ({false, _N}) -> false;
            (Obj) -> {true, Obj}
        end,
        
    Page1 = 1,
    K1 = get_pages(PagesId2, Page1, PageSize),
    K2 = lists:filtermap(MapRedFun, K1),
    K2Len = length(K2),
    redefine_pages(PagesId2, Page1, PageSize, K2),
    Page2 = Page1 + K2Len,
    K3 = get_pages(PagesId2, Page2, PageSize),
    K4 = lists:filtermap(MapRedFun, K3),
    K4Len = length(K4),
    redefine_pages(PagesId2, Page2, PageSize, K4),
    Page3 = Page2 + K4Len,
    K5 = get_pages(PagesId2, Page3, PageSize),
    K6 = lists:filtermap(MapRedFun, K5),
    redefine_pages(PagesId2, Page3, PageSize, K6),

    [{{query, PagesId2}, AfterProcessKeys}] = 
        ets:lookup(?MODULE, {query, PagesId2}),
    [{true,1},{true,2},{true,4},{true,5},
    {true,8},{true,9},{true,10}] =
        AfterProcessKeys.







    




