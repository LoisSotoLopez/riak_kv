-define(HEADER_FREQ,20).

%% RECORDS
-record(qriak_response, {
    info :: qriak_respinfo:respinfo(),
    items :: responses_list()}).  

%% TYPES
-type response_item() :: [{string(), [binary()]}].
-type responses_list() :: [] | [response_item()].
-type qriak_response() :: {error, atom()} | {error, binary()} | #qriak_response{}.
-type field_key() :: string().
-type field_value() :: binary().
-type qriak_condition() :: 
  {is_and, qriak_conditions()}
  | {is_or, qriak_conditions()}
  | {field_key(), is, field_value()}
  | {field_key(), reads_like, field_value()}
  | {field_key(), colour_like, field_value()}
  | {field_key(), in_range, {field_value(), field_value()}}.
-type qriak_conditions() :: [] | [qriak_condition()].