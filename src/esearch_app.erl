%% file: esearch_app.erl
%% @author liubin <tryanswer@gmail.com>
%% @doc elasticsearch engine interface erlang application
%% @copyright 2012-09-12 liubin
%% @end.

-module(esearch_app).

-behaviour(application).

-export([start/2, stop/1]).

%% @hidden
start(_, _)->
    esearch_sup:start_link().

%% @hidden
stop(_)->
    application:stop(esearch).
