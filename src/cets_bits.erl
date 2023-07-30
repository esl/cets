%% Functions for bit operations
-module(cets_bits).
-export([set_flag/2]).
-export([unset_flag_mask/1]).
-export([apply_mask/2]).

-spec set_flag(Pos :: non_neg_integer(), Bits :: integer()) -> Bits :: integer().
set_flag(Pos, Bits) ->
    Bits bor (1 bsl Pos).

-spec unset_flag_mask(Pos :: non_neg_integer()) -> Mask :: integer().
unset_flag_mask(Pos) ->
    bnot (1 bsl Pos).

-spec apply_mask(Mask :: integer(), Bits :: integer()) -> Bits :: integer().
apply_mask(Mask, Bits) ->
    Bits band Mask.
