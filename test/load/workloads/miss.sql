-- Change a harmless predicate to generate mostly distinct SQL cache keys.
-- This exercises backend reads AND cache population, not a disabled cache.
\set nonce random(1, 2147483647)
SELECT id, username, email FROM public.users WHERE id = 1 AND :nonce >= 0;
