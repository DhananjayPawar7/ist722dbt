with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_product as (
    select * from {{ ref('dim_product') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_employee as (
    select * from {{ ref('dim_employee') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
)
select
    -- Customer dimensions
    d_customer.customerkey,
    d_customer.customerid,
    d_customer.companyname,
    d_customer.contactname,
    d_customer.contacttitle,
    d_customer.address,
    d_customer.city,
    d_customer.region,
    d_customer.postalcode,
    d_customer.country,
    d_customer.phone,
    d_customer.fax,
    
    -- Employee dimensions
    d_employee.employeekey,
    d_employee.employeeid,
    d_employee.employeenamelastfirst,
    d_employee.employeenamefirstlast,
    d_employee.employeetitle,
    d_employee.supervisornamelastfirst,
    d_employee.supervisornamefirstlast,
    
    -- Product dimensions
    d_product.productkey,
    d_product.productid,
    d_product.productname,
    d_product.categoryid,
    d_product.categoryname,
    d_product.categorydescription,
    d_product.supplierid,
    d_product.suppliername,
    d_product.suppliercontactname,
    d_product.suppliercontacttitle,
    d_product.unitsinstock,
    d_product.unitsonorder,
    d_product.reorderlevel,
    d_product.discontinued,
    
    -- Date dimensions
    d_date.datekey,
    d_date.date,
    d_date.year,
    d_date.month,
    d_date.quarter,
    d_date.day,
    d_date.dayofweek,
    d_date.weekofyear,
    d_date.dayofyear,
    d_date.quartername,
    d_date.monthname,
    d_date.dayname,
    d_date.weekday,
    
    -- Sales facts
    f_sales.saleskey,
    f_sales.orderid,
    f_sales.orderdatekey,
    f_sales.quantity,
    f_sales.unitprice,
    f_sales.discount,
    f_sales.extendedprice,
    f_sales.discountamount,
    f_sales.salesamount
    
from f_sales
    left join d_product on f_sales.productkey = d_product.productkey
    left join d_customer on f_sales.customerkey = d_customer.customerkey
    left join d_employee on f_sales.employeekey = d_employee.employeekey
    left join d_date on f_sales.orderdatekey = d_date.datekey