-- One autocommit SELECT per iteration; no explicit transaction block.
SELECT id, username, email FROM public.users WHERE id = 1;
