with stg_orders as (
    select
        orderid,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey
    from {{ source('northwind','Orders') }}
),
stg_order_details as (
    select
        orderid,
        productid,
        unitprice,
        quantity,
        discount
    from {{ source('northwind','Order_Details') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['od.orderid','od.productid']) }} as saleskey,
    od.orderid,
    {{ dbt_utils.generate_surrogate_key(['od.productid']) }} as productkey,
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    od.quantity,
    od.unitprice,
    od.discount,
    od.quantity * od.unitprice as extendedprice,
    od.quantity * od.unitprice * od.discount as discountamount,
    od.quantity * od.unitprice * (1 - od.discount) as salesamount
from stg_order_details od
    inner join stg_orders o on od.orderid = o.orderid