-- The lines of an order must add up to the order total on its header, to the
-- cent. Orders whose lines haven't loaded yet are reported by
-- assert_order_headers_have_lines instead.

select
    order_id,
    line_count,
    net_revenue,
    order_total
from {{ ref('fact_order_headers') }}
where
    has_lines
    and abs(net_revenue - order_total) > 0.01
