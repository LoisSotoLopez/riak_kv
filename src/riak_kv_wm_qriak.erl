%% -------------------------------------------------------------------
%%
%% riak_kv_wm_bucket_type: Webmachine resource for qriak queries
%%
%% -------------------------------------------------------------------

%% @doc Resource for invoking qriak queries over HTTP.
%%
%% Available operations:
%%
%% ```
%% GET /qriak
%%
%% Will return object identifier and queried fields for all objects
%% matching the query provided as parameter, as well as information
%% related to the query result.
%%
%% Parameters to pass:
%% query - qriak DSL query
%% 
%% ```

-module(riak_kv_wm_qriak).

%% includes
-include("qriak.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% webmachine resource exports
-export([
    init/1,
    content_types_provided/2,
    allowed_methods/2
]).

%% request dispatching functions export
-export([
    execute_qriak_query/2
]).

%% Context record
-record(ctx, {bucket_type,  %% binary() - Bucket type (from uri)
              client,       %% riak_client() - the store client
              prefix,       %% string() - prefix for resource uris
              riak,         %% local | {node(), atom()} - params for riak client
              bucketprops,  %% proplist() - properties of the bucket
              method,       %% atom() - HTTP method for the request
              api_version,  %% non_neg_integer() - old or new http api
              security     %% security context
             }).
-type context() :: #ctx{}.

%% -------------------------------------------------------------------
%% RESOURCE EXPORTS
%% -------------------------------------------------------------------

-spec init(proplists:proplist()) -> {ok, context()}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Config) ->
    {ok, #ctx{
        prefix=proplists:get_value(prefix, Config),
        riak=proplists:get_value(riak, Config),
        api_version=proplists:get_value(api_version,Config),
        bucket_type=proplists:get_value(bucket_type, Config)
    }}.

-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc Produces content type to function name dispatcher association.
%%      "application/json" is the content-type for props requests.
content_types_provided(RD, Ctx) ->
    {[{"application/json", execute_qriak_query}], RD, Ctx}.

-spec allowed_methods(#wm_reqdata{}, context()) ->
    {[atom()], #wm_reqdata{}, context()}.
%% @doc Get the list of methods this resource supports.
%%      Properties allows HEAD, GET, and PUT.
allowed_methods(RD, Ctx) when Ctx#ctx.api_version =:= 3 ->
    {['GET'], RD, Ctx}.


%% -------------------------------------------------------------------
%% REQUEST DISPATCHING FUNCTIONS
%% -------------------------------------------------------------------

-spec execute_qriak_query(#wm_reqdata{}, context()) ->
    {binary(), #wm_reqdata{}, context()}.
%% @doc Produce the JSON response to a qriak query.
execute_qriak_query(RD, #ctx{} = Ctx) ->
    Query = wrq:get_qs_value("query", RD),
    QueryOut = qriak:select(Query),
    {Code, Body} = produce_wm_out(QueryOut),
    JsonBody = mochijson2:encode([{<<"result">>, Body}]),
    case Code of
        200 ->
            {JsonBody, RD, Ctx};
        _ ->
            RD1 = wrq:set_response_code(Code, RD),
            RD2 = wrq:set_resp_body(JsonBody, RD1),
            {{halt, 400}, RD2, Ctx}
    end.

%% -------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%% -------------------------------------------------------------------
% produce_wm_out(#qriak_response{info = Info, items = Items}) ->
%     {200, 
%         [{<<"info">>, produce_response_info(Info)},
%         {<<"items">>, << "[", (produce_response_items(Items))/binary, "]" >>}]};
produce_wm_out(#qriak_response{info = Info, items = Items}) ->
    {200, 
        [{<<"info">>, Info},
        {<<"items">>, Items}]};
produce_wm_out({error, bad_format_query}) ->
    {400, [{<<"error_msg">>, <<"Badly formatted query">>}]};
produce_wm_out({error, Error}) ->
    {400, [{<<"error_msg">>, Error}]};
produce_wm_out(Other) ->
    {500, [{<<"error_msg">>, io_lib:format(<<"Unrecognized qriak response : ~p">>, [Other])}]}.

% produce_response_info(#{total_count := Count}) ->
%     [{<<"total_count">>, Count}].

% produce_response_items(Items) ->
%     produce_response_items(Items, <<>>).

% produce_response_items([H], Acc) when is_list(H)->
%     << Acc/binary, "{", (produce_response_items(H))/binary, "}" >>;
% produce_response_items([H | T], Acc) when is_list(H) ->
%     NewAcc = << Acc/binary, "{", (produce_response_items(H))/binary, "}," >>,
%     produce_response_items(T, NewAcc);
% produce_response_items([{K, [V]} = Item], Acc) ->
%     << Acc/binary, (produce_response_item(Item))/binary >>;
% produce_response_items([{K, [V]} = Item | Rest], Acc) ->
%     NewAcc =  << Acc/binary, (produce_response_item(Item))/binary, "," >>,
%     produce_response_items(Rest, NewAcc).

% produce_response_item({K, [V]}) ->
%     lists:flatten(io_lib:format("~s : ~s",[K, V])).

