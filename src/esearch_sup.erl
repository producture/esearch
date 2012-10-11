%% file: esearch_sup.erl
%% @author liubin <tryanswer@gmail.com>
%% @doc erlang elasticsearch supervisor.
%% @copyright 2012 liubin
%% @end.


-module(esearch_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

%% @hidden
start_link()->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%% @hidden
init(_Args)->
    ES = {esearch, {esearch, start_link, []}, permanent, brutal_kill, worker, [esearch]},
    Rs = {one_for_one, 10, 1},
    {ok, {Rs, [ES]}}.
