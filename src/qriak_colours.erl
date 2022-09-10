-module(qriak_colours).

%% MACROS
-define(COLOURS,([
    {<<"black">>, {0, 0, 0}}, 
    {<<"white">>, {255, 255, 255}}, 
    {<<"red">>, {255, 0, 0}}, 
    {<<"lime">>, {0, 255, 0}}, 
    {<<"blue">>, {0, 0, 255}}, 
    {<<"yellow">>, {255, 255, 0}}, 
    {<<"cyan">>, {0, 255, 255}}, 
    {<<"magenta">>, {255, 0, 255}}, 
    {<<"silver">>, {192, 192, 192}}, 
    {<<"gray">>, {128, 128, 128}}, 
    {<<"maroon">>, {128, 0, 0}}, 
    {<<"olive">>, {128, 128, 0}}, 
    {<<"green">>, {0, 128, 0}}, 
    {<<"purple">>, {128, 0, 128}}, 
    {<<"teal">>, {0, 128, 128}}, 
    {<<"navy">>, {0, 0, 128}}])).

%% RECORDS
-record(colour, {r :: 0..255, g :: 0..255, b :: 0..255}).

%% EXTERNAL EXPORTS
-export([
    init/0,
    are_similar/2
]).

%%-------------------------------------
%% EXTERNAL EXPORTS
%%-------------------------------------

init() ->
    lists:foreach(
        fun(Colour) -> gen_similar_colours(Colour) end,
        ?COLOURS).

are_similar(C1, C2) ->
    Similars = persistent_term:get({qriak_colour, C1}, []),
    lists:member(C2, Similars).

%%-------------------------------------
%% INTERNAL FUNCTIONS
%%-------------------------------------
colour_rd({Red, Green, Blue}) ->
    #colour{r = Red, g = Green, b = Blue}.
    
gen_similar_colours({ColourAlias, ColourTuple}) ->
    Orig = colour_rd(ColourTuple),
    Similars = 
        lists:filtermap(
            fun({CAlias, CTuple}) -> 
                Dst = colour_rd(CTuple),
                case colour_distance(Orig, Dst) of
                    N when N < 300 ->
                        {true, CAlias};
                    _ ->
                        false
                end
            end,
            ?COLOURS),
    store_similar_colours(ColourAlias, Similars).

store_similar_colours(ColourAlias, Similars) ->
    ok = persistent_term:put({qriak_colour, ColourAlias}, Similars).


colour_distance(C1, C2) ->
    % Formula taken from https://www.compuphase.com/cmetric.htm
    % Licensed under https://creativecommons.org/licenses/by-sa/3.0/
    Rmean = (C1#colour.r + C2#colour.r) / 2,
    R = C1#colour.r - C2#colour.r,
    G = C1#colour.g - C2#colour.g,
    B = C1#colour.b - C2#colour.b,
    math:sqrt((((512 + Rmean)*R*R) / math:pow(2, 8)) + 4*G*G + (((767-Rmean)*B*B) / math:pow(2, 8))).

