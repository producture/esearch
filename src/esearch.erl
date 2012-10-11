%% file: esearch.erl
%% @author liubin <tryanswer@gmail.com>
%% @doc elasticsearch engine erlang interface.
%% @copyright 2012 liubin
%% @end.

-module(esearch).

-export([start_link/0, init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


-export([
	 %% core
	 index/6, 
	 exist/5, 
	 delete/5, 
	 get/5, 
	 multi_get/5,
	 update/6, 
	 search_req_body/5, 
	 search_uri_req/4, 
	 multi_search/5,
	 percolate/5, 
	 percolate_doc/5, 
	 delete_percolate/3, 
	 bulk/5, 
	 count/5,
	 delete_by_query/4,
	 more_like_this/5,
	 validate/5,
	 explain/6,

	 %% indices
	 aliase_index/2,
	 create_index/4,
	 delete_index/2,
	 analyze/4,
	 open_index/2,
	 close_index/2,
	 get_settings/2,
	 update_settings/4,
	 get_mapping/3,
	 put_mapping/5,
	 delete_mapping/3,
	 refresh/2,
	 optimize/3,
	 flush_and_fresh/2,
	 flush_and_nofresh/2,
	 snapshot/2,
	 template/3,
	 delete_template/2,
	 get_template/2,
	 stats/3,
	 status/3,
	 segments/2,
	 clear_cache/3,
	 

	 %% cluster
	 health/3,
	 state/2,
	 cluster_update_settings/2,
	 get_cluster_settings/1,
	 nodes_info/3,
	 nodes_stats/3,
	 shutdown_nodes/3,
	 hot_threads/3
	]).

-define(FLATTEN(List), lists:flatten(List)).
-define(JSON(Json), if Json == ""-> ""; true-> e_json:encode(Json) end).

%% @doc index document. if Docid == "", it will automatic generate ID.
%% Url: "http://localhost:9200/".<br></br>
%% Index: index.<br></br>
%% Type: type.<br></br>
%% Params: [{key, value}, ...].<br></br>
index(Url, Index, Type, Docid, Params, Json)->
    [Tag, FullUrl] = if Docid == "" orelse Docid == undefined->
			     [post, ?FLATTEN([Url, Index, "/", Type])];
			true->
			     Bool = hasRouting(Params),
			     if Bool->
				     T = post;
				true->
				     T = put
			     end,
			     [T, ?FLATTEN([Url, Index, "/", Type, "/", Docid])]
		     end,
    gen_server:call(?MODULE, {Tag, FullUrl, Params, Json}).
hasRouting(Params)->
    proplists:get_value(routing, Params) =/= 0 orelse
	proplists:get_value("routing", Params) =/= 0.


%% @doc check typed JSON document exist
%% return true| false;
exist(Url, Index, Type, Docid, Params)->
    case gen_server:call(?MODULE, {head, ?FLATTEN([Url, Index, "/", Type, "/", Docid]), Params}) of
	{ok, _}->
	    true;
	_ ->
	    false
    end.

%% @doc delete typed JSON document.
%% return ok| {error, Reason}.
delete(Url, Index, Type, Docid, Params)->
    case gen_server:call(?MODULE, {delete, ?FLATTEN([Url, Index, "/", Type, "/", Docid]), Params}) of
	{ok, _}->
	    ok;
	R ->
	    R
    end.


%% @doc get typed JSON document.
%% set Json = [] to per get.
get(Url, Index, Type, Docid, Params)->
    if Index == "" orelse Index == undefined orelse Type == "" orelse Type == undefined->
	    {error, "illegal index/type args"};
       true->
	    gen_server:call(?MODULE, {get, ?FLATTEN([Url, Index, "/", Type, "/", Docid]), Params})
    end.
multi_get(Url, Index, Type, Params, Json)->
    NewUrl = if Index =/= "" orelse Index =/= undefined->
		     if Type =/= "" orelse Type =/= undefined->
			     ?FLATTEN([Url, Index, "/", Type, "/_mget"]);
			true ->
			     ?FLATTEN([Url, Index, "/_mget"])
		     end;
		true ->
		     Url ++ "_mget"
	     end,
    gen_server:call(?MODULE, {post, NewUrl, Params, Json}).

%% @doc update update a document based on script provided.
update(Url, Index, Type, Docid, Params, Json)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, Index, "/", Type, "/", Docid, "/_update"]), Params, Json}).


%% @doc search api - request body.
%% Indexes: [index, index, ...].
%% Types: [type, type, ...].
search_req_body(Url, Indexes, Types, Params, Json)->
    Center = gen_search_url(Indexes, Types),
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, Center, "_search"]), Params, Json}).

%% @doc search api - URI Request.
%% Indexes: [index, index, ...].
%% Types: [type, type, ...].
search_uri_req(Url, Indexes, Types, Params)->
    Center = gen_search_url(Indexes, Types),
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, Center, "_search"]), Params}).

%% @doc multi search.
%% Indexes: [index, ...].
%% Types: [type, ...].
%% Json: [{Head, Body}, ...]
multi_search(Url, Indexes, Types, Params, Json)->
    Center = gen_search_url(Indexes, Types),
    gen_server:call(?MODULE, {bulk_post, ?FLATTEN([Url, Center, "_msearch"]), Params, Json}).

gen_search_url(Indexes, Types)->
    Ies = lists:flatten(lists:foldl(fun(E, Acc)->
					    [","| [E| Acc]]
				    end, [], lists:reverse(Indexes))),
    Ies1 = if Ies == ""->
		   "";
	      true ->
		   string:sub_string(Ies, 2, length(Ies)) ++ "/"
	   end,
    Tes = lists:flatten(lists:foldl(fun(E, Acc)->
					    [","| [E| Acc]]
				    end, [], lists:reverse(Types))),
    Tes1 = if Tes == ""->
		   Ies1;
	      true ->
		   Ies1 ++ string:sub_string(Tes, 2, length(Tes)) ++ "/"
	   end,
    Tes1.

%% @doc register queries against an index, and then send percolate requests which include a doc, and geting back the queries that match on that doc out of the set of registed queries.
percolate(Url, Index, PercolateName, Params, Json)->
    gen_server:call(?MODULE, {put, ?FLATTEN([Url, "_percolator/", Index, "/", PercolateName]), Params, Json}).

%% @doc percolate document, get back matched queries.
percolate_doc(Url, Index, Type, Params, Json)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, Index, "/", Type, "/_percolate"]), Params, Json}).

%% @doc delete percolator.
delete_percolate(Url, Index, PercolateName)->
    gen_server:call(?MODULE, {delete, ?FLATTEN([Url, "_percolator/", Index, "/", PercolateName]), []}).


%% @doc bulk.
%% Json: [{action_and_meta_data, document}, ...]
bulk(Url, Index, Type, Params, Json)->
    gen_server:call(?MODULE, {bulk_post, ?FLATTEN([Url, gen_search_url([Index], [Type]), "_bulk"]), Params, Json}).

%% @doc count query.
%% Indexes: [Index, ...].
%% Types: [Type, ...].
count(Url, Indexes, Types, Params, Json)->
    if Json == "" orelse Json == undefined->
	    gen_server:call(?MODULE, {get, ?FLATTEN([Url, gen_search_url(Indexes, Types), "_count"]), Params});
       true ->
	    gen_server:call(?MODULE, {post, ?FLATTEN([Url, gen_search_url(Indexes, Types), "_count"]), Params, Json})
    end.

%% @doc delete by query.
%% Indexes: [index, ...].
%% Types: [type, ...].
%% Attension: this function doesn't support query body.
delete_by_query(Url, Indexes, Types, Params)->
    gen_server:call(?MODULE, {delete, ?FLATTEN([Url, gen_search_url(Indexes, Types), "_query"]), Params}).

%% @doc more like this.
more_like_this(Url, Index, Type, "", Params)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, Index, "/", Type, "/_mlt"]), Params});
more_like_this(Url, Index, Type, Docid, Params)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, Index, "/", Type, "/", Docid, "/_mlt"]), Params}).


%% @doc validate a potentially expensive query
validate(Url, Index, Type, Params, Json)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, gen_search_url([Index], [Type]),  "_validate/query"]), Params, Json}).

%% @doc explain.
%% I didn't test this function!
explain(Url, Index, Type, Docid, Params, Json)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, Index, "/", Type, "/", Docid, "/_explain"]), Params, Json}).



%% @doc aliase index
aliase_index(Url, Json)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, "_aliases"]), [], Json}).


%% @doc create index
create_index(Url, Index, Params, Json)->
    gen_server:call(?MODULE, {put, ?FLATTEN([Url, Index]), Params, Json}).

%% @doc delete index
delete_index(Url, Index)->
    gen_server:call(?MODULE, {delete, ?FLATTEN([Url, Index]), []}).

%% @doc analyze on a text, and return the tokens breakdown of the text.
analyze(Url, Index, Params, Json)->
    if Index == "" orelse Index == undefined->
	    gen_server:call(?MODULE, {post, ?FLATTEN([Url, "_analyze"]), Params, Json});
       true ->
	    gen_server:call(?MODULE, {post, ?FLATTEN([Url, Index, "/_analyze"]), Params, Json})
    end.

%% @doc open index
open_index(Url, Index)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, Index, "/_open"]), [], []}).

%% @doc close index
close_index(Url, Index)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, Index, "/_close"]), [], []}).

%% @doc get setting
get_settings(Url, Index)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, Index, "/_settings"]), []}).

%% @doc update settings
update_settings(Url, Index, Params, Json)->
    gen_server:call(?MODULE, {put, ?FLATTEN([Url, Index, "/_settings"]), Params, Json}).

%% @doc get mapping
%% Indexes: [index, ...].
%% Types: [type, ...].
get_mapping(Url, Indexes, Types)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, gen_search_url(Indexes, Types), "_mapping"]), []}).

%% @doc put mapping.
%% Indexes: [index, ...].
put_mapping(Url, Indexes, Type, Params, Json)->
    gen_server:call(?MODULE, {put, ?FLATTEN([Url, gen_search_url(Indexes, [Type]), "_mapping"]), Params, Json}).

%% @doc delete mapping
delete_mapping(Url, Index, Type)->
    gen_server:call(?MODULE, {delete, ?FLATTEN([Url, Index, "/", Type]), []}).

%% @doc refresh indexes.
%% Indexes: [index, ...].
refresh(Url, Indexes)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, gen_search_url(Indexes, []), "_refresh"]), [], []}).

%% @doc optimize indexes.
%% Indexes: [index, ...].
optimize(Url, Indexes, Params)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, gen_search_url(Indexes, []), "_optimize"]), Params, []}).


%% @doc flush.
%% Indexes: [index, ...].
flush_and_fresh(Url, Indexes)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, gen_search_url(Indexes, []), "_flush?refresh=true"]), [], []}).
flush_and_nofresh(Url, Indexes)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, gen_search_url(Indexes, []), "_flush?refresh=false"]), [], []}).

%% @doc snapshot/ backup.
%% Indexes: [index, ...].
snapshot(Url, Indexes)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, gen_search_url(Indexes, []), "_gateway/snapshot"]), [], []}).

%% @doc Index templates allow to define templates that will automatically be applied to new indices created.
template(Url, Tempname, Json)->
    gen_server:call(?MODULE, {put, ?FLATTEN([Url, "_template/", Tempname]), [], Json}).

%% @doc delete template.
delete_template(Url, Tempname)->
    gen_server:call(?MODULE, {delete, ?FLATTEN([Url, "_template/", Tempname]), []}).

%% @doc get template.
get_template(Url, Tempname)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, "_template/", Tempname]), []}).

%% @doc stats provide statistics on different operations happening on an index
%% Indexes: [index, ...].
stats(Url, Indexes, Params)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, gen_search_url(Indexes, []), "_stats"]), Params}).

%% @doc get a comprehensive status information of one or more indices.
%% Indexes: [index, ...].
status(Url, Indexes, Params)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, gen_search_url(Indexes, []), "_status"]), Params}).

%% @doc Provide low level segments information that a Lucene index (shard level) is built with.
%% Indexes: [index, ...].
segments(Url, Indexes)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, gen_search_url(Indexes, []), "_segments"]), []}).

%% @doc clear cache.
%% Indexes: [index, ...].
clear_cache(Url, Indexes, Params)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, gen_search_url(Indexes, []), "_cache/clear"]), Params, []}).



%% @doc cluster health.
health(Url, Indexes, Params)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, "_cluster/health/", gen_search_url(Indexes, [])]), Params}).

%% @doc cluster state
state(Url, Params)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, "_cluster/state"]), Params}).

%% @doc update cluster wide specific settings.
cluster_update_settings(Url, Json)->
    gen_server:call(?MODULE, {put, ?FLATTEN([Url, "_cluster/settings"]), [], Json}).

%% @doc get cluster settings.
get_cluster_settings(Url)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, "_cluster/settings"]), []}).

%% @doc nodes info.
nodes_info(Url, Nodes, Params)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, "_cluster/nodes/", gen_search_url(Nodes, [])]), Params}).

%% @doc nodes stats
nodes_stats(Url, Nodes, Params)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, "_cluster/nodes/", gen_search_url(Nodes, []), "stats"]), Params}).

%% @doc shut nodes
shutdown_nodes(Url, Nodes, Params)->
    gen_server:call(?MODULE, {post, ?FLATTEN([Url, "_cluster/nodes/", gen_search_url(Nodes, []), "_shutdown"]), Params, []}).

%% @doc get the current hot threads on each node in the cluster.
hot_threads(Url, Nodes, Params)->
    gen_server:call(?MODULE, {get, ?FLATTEN([Url, "_nodes/", gen_search_url(Nodes, []), "hot_threads"]), Params, []}).

%% -------------------------------------------------------------------------------------gen_server
%% @hidden
start_link()->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
%% genserver call back functions
%% @hidden
init(_Args)->
    {ok, "esearch"}.
%% @hidden
handle_info(_info, State) ->
    {noreply, State}.
%% @hidden
terminate(_Reason, _State)->
    ok.
%% @hidden
code_change(_VSN, State, _Extra) ->
    {ok, State}.
%% @hidden
handle_cast(_Req, State)->
    {noreply, State}.
%% @hidden
handle_call({put, Url, Params, Json}, _From, State)->
    JsonStr = ?JSON(Json),
    R = ehttp:'PUT'(Url, Params, JsonStr),
    {reply, R, State};
handle_call({post, Url, Params, Json}, _From, State) ->
    JsonStr = ?JSON(Json),
    R = ehttp:'POST'(Url, Params, JsonStr),
    {reply, R, State};
handle_call({bulk_post, Url, Params, Json}, _From, State) ->
    JsonStr = lists:reverse(lists:foldl(fun({Action, Doc}, Acc)->
						[[?JSON(Action), "\n", ?JSON(Doc), "\n"]| Acc];
					   (E, Acc) ->
						error_logger:error_msg("~p module, illegal Json Element in bulk/5: ", [?MODULE, E]),
						Acc
					end, [], Json)),
    R = ehttp:'POST'(Url, Params, list_to_binary(JsonStr)),
    {reply, R, State};
handle_call({get, Url, Params}, _From, State) ->
    R = ehttp:'GET'(Url, Params),
    {reply, R, State};
handle_call({delete, Url, Params}, _From, State) ->
    R = ehttp:'DEL'(Url, Params),
    {reply, R, State};
handle_call({head, Url, Params}, _From, State) ->
    R = ehttp:'HEAD'(Url, Params),
    {reply, R, State}.
