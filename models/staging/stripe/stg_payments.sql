{{
    config(
        materialized='view',
        tags=['finance'],
        docs={'node_color': 'red'},
        alias='stg_payments'
    )
}}

with

source as (
    select * from {{ source('stripe', 'payment') }}
),

payments as (
    
    select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,

    -- amount is stored in cents, convert it to dollars
    amount / 100 as amount,
    -- using macro to do the same
    {{ cents_to_dollars('amount', 1) }} as amount_usd,
    created as created_at

from source

)

 select * from payments
