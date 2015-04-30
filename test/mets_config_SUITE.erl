-module(mets_config_SUITE).
-author('Lukasz Biedrycki <lukasz.biedrycki@gmail.com>').

-export([all/0, suite/0]).
-export([t_prop_config/1]).

-include("mets.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(NUMTESTS, 1000).
-define(PROPTEST(A), true = proper:quickcheck(A(),
                                              [{numtests, ?NUMTESTS},
                                               {constraint_tries, 1000}])).


suite() ->
    [{timetrap, {seconds, 40}}].

all() ->
    [
        t_prop_config
    ].

%% Test codec symmetrically
t_prop_config(_Config) ->
    ?PROPTEST(prop_config).

%% PropEr
prop_config() ->
    ?FORALL(Rules, rules(),
           begin
               Dispatch = mets_config:compile(Rules),
               case Rules of
                   [] ->
                       true;
                   [{Ns, Name, Tags0, Aggregate, Options}|_] ->
                       Tags = case Tags0 of
                                  '_' -> [{<<"key">>, <<"value">>}];
                                  _ -> Tags0
                              end,
                       Results = mets_config:do_match(Dispatch, Ns, Name, Tags),
                       lists:member({Aggregate, Options}, Results)
               end
           end).

rules() ->
    list(rule()).

rule() ->
    {oneof([ns(),'_']),
     oneof([name(),'_']),
     oneof([tags(),'_']),
     aggregate(),
     options()}.

ns()   -> non_empty(utf8_bin()).
name() -> non_empty(utf8_bin()).

options() -> ?LET(Opts,
                  list(oneof([{ttl, pos_integer()}])),
                  dict:to_list(dict:from_list(Opts))).

tags() ->
    list(?LET({K,V}, {oneof([utf8_bin(),'_']), oneof([utf8_bin(),'_'])}, {K,V})).

aggregate() ->
    ?LET({F, P}, {stat(), {pos_integer(), units()}}, {F, P}).

stat() ->
    oneof([avg, sum, min, max, count]).

units() ->
    oneof([seconds, minutes, hours, days, weeks]).

utf8_bin() ->
    ?LET(S,
         non_empty(list(oneof([integer(16#30, 16#39),
                               integer(16#41, 16#5A),
                               integer(16#61, 16#7A)]))),
         list_to_binary(S)).
