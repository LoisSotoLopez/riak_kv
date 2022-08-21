%%% @doc Adaptation of the riak_ql_parser.yrl parser on 
%%% https://github.com/basho/riak_ql/blob/develop/src/riak_ql_parser.yrl
%%% Doc below left intact for credits

%%% @doc       Parser for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2016, 2017 Basho

Nonterminals

BetweenPredicate
Bucket
BooleanValueExpression
BooleanPredicand
BooleanPrimary
BooleanTerm
BooleanTest
CharacterLiteral
Comp
ComparisonPredicate
From
Identifier
IsPredicate
Query
Statement
StatementWithoutSemicolon
Where
.

Terminals

and_
between
character_literal
colour_like
from
identifier
is_
left_paren
or_
reads_like
right_paren
where

.

Rootsymbol Statement.
Endsymbol '$end'.

Statement -> StatementWithoutSemicolon : '$1'.

StatementWithoutSemicolon -> Query           : convert('$1').

Query -> From  : '$1'.

From -> from Bucket Where : make_from('$1', '$2', '$3').

Bucket -> Identifier   : '$1'.

Identifier -> identifier : '$1'.

Where -> where BooleanValueExpression : make_where('$1', '$2').

BooleanValueExpression -> BooleanTerm : '$1'.
BooleanValueExpression ->
    BooleanValueExpression or_ BooleanTerm :
        {expr, {or_, '$1', '$3'}}.

BooleanTerm -> BooleanTest : '$1'.
BooleanTerm ->
    BooleanTerm and_ BooleanTest :
        {expr, {and_, '$1', '$3'}}.


BooleanTest -> BooleanPrimary : '$1'.

BooleanPrimary -> BooleanPredicand : '$1'.

BooleanPredicand ->
    IsPredicate : '$1'.
BooleanPredicand ->
    ComparisonPredicate : '$1'.
BooleanPredicand ->
    BetweenPredicate : '$1'.
BooleanPredicand ->
    left_paren BooleanValueExpression right_paren : '$2'.

IsPredicate -> Identifier is_ CharacterLiteral : make_is_predicate('$1', ['$3']).
BetweenPredicate -> Identifier between CharacterLiteral CharacterLiteral : make_between_predicate('$1', '$3', '$4').
ComparisonPredicate -> Identifier Comp CharacterLiteral : make_expr('$1', '$2', '$3').

Comp -> reads_like : '$1'.
Comp -> colour_like : '$1'.

CharacterLiteral -> character_literal : character_literal_to_binary('$1').

Erlang code.

-record(outputs,
        {
          type :: select,
          buckets = [],
          where   = []
         }).

%% export the return value function to prevent xref errors
%% this fun is used during the parsing and is marked as
%% unused/but not to be exported in the yecc source
%% no way to stop rebar borking on it AFAIK
-export([
         return_error/2,
         ql_parse/1
         ]).

%% Provide more useful success tuples
ql_parse(Tokens) ->
    interpret_parse_result(parse(Tokens)).

interpret_parse_result({error, _}=Err) ->
    Err;
interpret_parse_result({ok, Proplist}) ->
    extract_type(proplists:get_value(type, Proplist), Proplist).

extract_type(Type, Proplist) ->
    {Type, Proplist -- [{type, Type}]}.

convert(#outputs{type     = from,
                 buckets  = B,
                 where    = W}) ->
    [
     {tables,   B},
     {where,    W}
    ].

make_from({from, _FromBytes},
            {_Type, D},
            {_Where, E}) ->
    Bucket = D,

    #outputs{type    = from,
             buckets = Bucket,
             where   = E
            }.

make_is_predicate(_, []) ->
    return_error_flat("IS filters must have one value.");
make_is_predicate({identifier, Identifier}, [{character_literal, CharacterLiteral}]) ->
    {expr, {is_, Identifier, CharacterLiteral}}.

make_between_predicate({identifier, A}, {character_literal, B}, {character_literal, C}) ->
    {expr, {between, A, B, C}}.


make_expr({identifier, A}, {B, _}, {character_literal, C}) ->
    {expr, {B, A, C}}.


make_where({where, A}, {expr, B}) ->
    NewB = remove_exprs(B),
    {A, [NewB]}.

remove_exprs({expr, A}) ->
    remove_exprs(A);
remove_exprs({A, B, C}) ->
    {A, remove_exprs(B), remove_exprs(C)};
remove_exprs(A) ->
    A.

character_literal_to_binary({character_literal, CharacterLiteralBytes})
  when is_binary(CharacterLiteralBytes) ->
    {character_literal, CharacterLiteralBytes}.


-spec return_error_flat(string()) -> no_return().
return_error_flat(F) ->
    return_error_flat(F, []).

-spec return_error_flat(string(), [term()]) -> no_return().
return_error_flat(F, A) ->
    return_error(
      0, iolist_to_binary(io_lib:format(F, A))).
