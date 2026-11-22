-- Warns about order headers whose lines haven't loaded yet (for example, the
-- insert transaction arrived after the extract cutoff while a later status
-- change didn't). A self-healing gap: the lines arrive with a later load.

{{ config(severity='warn') }}

select
    order_id,
    order_status,
    order_placed_at
from {{ ref('fact_order_headers') }}
where not has_lines
