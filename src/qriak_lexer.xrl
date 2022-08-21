%%% @doc Adaptation of the riak_ql_lexer.lex lexer on 
%%% https://github.com/basho/riak_ql/blob/develop/src/riak_ql_lexer.lex
%%% Doc below left intact for credits

%%% @doc       Lexer for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2016 Basho

Definitions.

AND = (A|a)(N|n)(D|d)
BETWEEN = (B|b)(E|e)(T|t)(W|w)(E|e)(E|e)(N|n)
COLOUR_LIKE = (C|c)(O|o)(L|l)(O|o)(U|u)(R|r)(_)(L|l)(I|i)(K|k)(E|e)
FROM = (F|f)(R|r)(O|o)(M|m)
IS = (I|i)(S|s)
OR = (O|o)(R|r)
READS_LIKE = (R|r)(E|e)(A|a)(D|d)(S|s)(_)(L|l)(I|i)(K|k)(E|e)
WHERE = (W|w)(H|h)(E|e)(R|r)(E|e)

CHARACTER_LITERAL = '(''|[^'\n])*'

IDENTIFIER = ([a-zA-Z][a-zA-Z0-9_\-]*)
WHITESPACE = ([\000-\s]*)

Rules.

{AND} : {token, {and_, list_to_binary(TokenChars)}}.
{FROM} : {token, {from, list_to_binary(TokenChars)}}.
{NOT} : {token, {not_, list_to_binary(TokenChars)}}.
{OR} : {token, {or_, list_to_binary(TokenChars)}}.
{WHERE} : {token, {where, list_to_binary(TokenChars)}}.


{IS} : {token, {is_, list_to_binary(TokenChars)}}.
{BETWEEN} : {token, {between, list_to_binary(TokenChars)}}.
{READS_LIKE}  : {token, {reads_like, list_to_binary(TokenChars)}}.
{COLOUR_LIKE}  : {token, {colour_like, list_to_binary(TokenChars)}}.

{CHARACTER_LITERAL} :
  {token, {character_literal, clean_up_literal(TokenChars)}}.

{WHITESPACE} : skip_token.

\n : {end_token, {'$end'}}.

{IDENTIFIER} : {token, {identifier, clean_up_identifier(TokenChars)}}.

.  : error(iolist_to_binary(io_lib:format("Unexpected token '~s'.", [TokenChars]))).

Erlang code.

-compile([nowarn_export_all,export_all]).

get_tokens(X) ->
    Toks = lex(X),
    post_process(Toks).

post_process(X) ->
    post_p(X, []).

%% filter out the whitespaces at the end
post_p([], Acc) ->
     lists:reverse(Acc);
post_p([{identifier, TokenChars} | T], Acc) when is_list(TokenChars)->
    post_p(T, [{identifier, list_to_binary(TokenChars)} | Acc]);
post_p([H | T], Acc) ->
    post_p(T, [H | Acc]).

lex(String) ->
    {ok, Toks, _} = string(String),
    Toks.

clean_up_identifier(Literal) ->
    clean_up_literal(Literal).

clean_up_literal(Literal) ->
    Literal1 = case hd(Literal) of
        $' -> accurate_strip(Literal, $');
        $" ->
            [error(unicode_in_quotes) || U <- Literal, U > 127],
            accurate_strip(Literal, $");
        _ -> Literal
    end,
    DeDupedInternalQuotes = dedup_quotes(Literal1),
    list_to_binary(DeDupedInternalQuotes).

%% dedup(licate) quotes, using pattern matching to reduce to O(n)
dedup_quotes(S) ->
    dedup_quotes(S, []).
dedup_quotes([], Acc) ->
    lists:reverse(Acc);
dedup_quotes([H0,H1|T], Acc) when H0 =:= $' andalso H1 =:= $' ->
    dedup_quotes(T, [H0|Acc]);
dedup_quotes([H0,H1|T], Acc) when H0 =:= $" andalso H1 =:= $" ->
    dedup_quotes(T, [H0|Acc]);
dedup_quotes([H|T], Acc) ->
    dedup_quotes(T, [H|Acc]).

%% only strip one quote, to accept Literals ending in the quote
%% character being stripped
accurate_strip(S, C) ->
    case {hd(S), lists:last(S), length(S)} of
        {C, C, Len} when Len > 1 ->
            string:substr(S, 2, Len - 2);
        _ ->
            S
    end.

fpsci_to_float(Chars) ->
    [Mantissa, Exponent] = re:split(Chars, "E|e", [{return, list}]),
    M2 = normalise_mant(Mantissa),
    E2 = normalise_exp(Exponent),
    sci_to_f2(M2, E2).

sci_to_f2(M2, E) when E =:= "+0" orelse
                      E =:= "-0" -> list_to_float(M2);
sci_to_f2(M2, E2) -> list_to_float(M2 ++ "e" ++ E2).

normalise_mant(Mantissa) ->
    case length(re:split(Mantissa, "\\.", [{return, list}])) of
        1 -> Mantissa ++ ".0";
        2 -> Mantissa
    end.

normalise_exp("+" ++ No) -> "+" ++ No;
normalise_exp("-" ++ No) -> "-" ++ No;
normalise_exp(No)        -> "+" ++ No.

fpdec_to_float([$- | _RemainTokenChars] = TokenChars) ->
    list_to_float(TokenChars);
fpdec_to_float([$. | _RemainTokenChars] = TokenChars) ->
    list_to_float([$0 | TokenChars]);
fpdec_to_float(TokenChars) ->
    list_to_float(TokenChars).
