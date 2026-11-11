-- net_revenue must equal quantity × unit_price × (1 − discount), to the cent.

select order_id, quantity, unit_price, discount_pct, net_revenue
from {{ ref('fact_orders') }}
where abs(net_revenue - round(quantity * unit_price * (1 - discount_pct), 2)) > 0.01
