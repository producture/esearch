-module(ehttp).

-export(['GET'/2, 'POST'/3, 'PUT'/3, 'DEL'/2, 'HEAD'/2]).

'GET'(Url, Params)->
    NewUrl = genUrl(Url, Params),
    request(get, {NewUrl, []}).
'POST'(Url, Params, JsonData)->
    NewUrl = genUrl(Url, Params),
    request(post, NewUrl, JsonData).
'PUT'(Url, Params, JsonData)->
    NewUrl = genUrl(Url, Params),
    request(put, NewUrl, JsonData).
'DEL'(Url, Params)->
    NewUrl = genUrl(Url, Params),
    request(delete, {NewUrl, []}).
'HEAD'(Url, Params)->
    NewUrl = genUrl(Url, Params),
    request(head, {NewUrl, []}).

genUrl(Url, Params) when is_list(Params)->
    Index = string:rstr(Url, "?"),
    Url1 = if Index == 0->
		   Url ++ "?";
	      true ->
		   Url ++ "&"
	   end,
    Args = lists:flatten(lists:foldl(fun({A, B}, Acc) when is_list(A) andalso is_list(B)->
					     [[A, "=", yaws_api:url_encode(B), "&"]| Acc];
					({A, B}, Acc) when is_atom(A) andalso is_list(B)->
					     [[atom_to_list(A), "=", yaws_api:url_encode(B), "&"]| Acc];
					({A, B}, Acc) when is_atom(A) andalso is_atom(B)->
					     [[atom_to_list(A), "=", yaws_api:url_encode(atom_to_list(B)), "&"]| Acc];
					({A, B}, Acc) when is_list(A) andalso is_atom(B) ->
					     [[A, "=", yaws_api:url_encode(atom_to_list(B)), "&"]| Acc];
					(E, Acc) ->
					     error_logger:error_msg("~p module, illegal params: ~p~n", [?MODULE, E]),
					     Acc
				     end, [], Params)),
    U = Url1 ++ Args,
    string:sub_string(U, 1, length(U) - 1).


request(Method, Url, JsonData)->
    request(Method, {Url, [], "application/json", JsonData}).
request(Method, Request)->
    case httpc:request(Method, Request, [], []) of
	{ok, {{_, 200, _}, "null"}}->
	    {ok, []};
	{ok, {{_, Status, _}, _, Json}} when Status == 200 orelse Status == 201->
	    try e_json:decode(Json) of
		R->
		    case R of
			{ok, []}->
			    {ok, []};
			{ok, [Data]}->
			    {ok, Data};
			{ok, {_, Data}}->
			    {ok, Data}
		    end
	    catch
		_:Err->
		    error_logger:error_msg("~p module, can not decode json data in request/4, json: ~n~p~n", 
					   [?MODULE, Err]),
		    {error, decode}
	    end;
	{ok, {{_, 404, _}, _, _}} ->
	    {error, not_found};
	{ok, {{_, 409, _}, _, Body}}->
	    error_logger:error_msg("~p module, conflict during request/4, result: ~n~p~n", 
				   [?MODULE, Body]),
	    {error, conflict};
	{ok, {{_, 400, _}, _, Body}}->
	    error_logger:error_msg("~p module, bad request during request/4, result: ~n~p~n", 
				   [?MODULE, Body]),
	    {error, bad_request};
	{ok, {{_, 503, _}, _, Body}}->
	    error_logger:error_msg("~p module, service unavailable during request/4, result: ~n~p~n", 
				   [?MODULE, Body]),
	    {error, service_unavailable};
	{ok, {{_, 500, _}, _, Body}}->
	    error_logger:error_msg("~p module, Internal Server Error during request/4, result: ~n~p~n", 
				   [?MODULE, Body]),
	    {error, internal_server_error};
	{error, Reason} ->
	    error_logger:error_msg("~p module, error during request/4, reason: ~p~n", 
				   [?MODULE, Reason])
    end.
