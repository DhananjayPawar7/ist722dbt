with stg_products as (
    select * from {{ source('northwind','Products') }}
),
stg_categories as (
    select * from {{ source('northwind','Categories') }}
),
stg_suppliers as (
    select * from {{ source('northwind','Suppliers') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['p.productid']) }} as productkey,
    p.productid,
    p.productname,
    p.categoryid,
    c.categoryname,
    c.description as categorydescription,
    p.supplierid,
    s.companyname as suppliername,
    s.contactname as suppliercontactname,
    s.contacttitle as suppliercontacttitle,
    p.unitprice,
    p.unitsinstock,
    p.unitsonorder,
    p.reorderlevel,
    p.discontinued
from stg_products p
    left join stg_categories c on p.categoryid = c.categoryid
    left join stg_suppliers s on p.supplierid = s.supplierid