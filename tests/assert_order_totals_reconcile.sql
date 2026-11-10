-- net_amount must equal quantity × unit_price × (1 − discount), to the cent.

select order_id, quantity, unit_price, discount_pct, net_amount
from {{ ref('fct_orders') }}
where abs(net_amount - round(quantity * unit_price * (1 - discount_pct), 2)) > 0.01
