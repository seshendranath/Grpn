set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.created.files=500000;

insert overwrite table dw.mv_fact_collections_master partition (ds)
SELECT 
collection_id,
collection_date_key,
order_id,
order_date_key,
order_created_date,
user_key,
deal_option_key,
deal_key,
division_key,
domain_key,
merchant_key,
country_key,
lead_parameters_key,
is_gift,
payment_type,
action,
user_source_key,
order_lead_id,
order_hour,
cost_to_groupon,
cost_to_user,
point_category,
discount_key,
credit_card_fee_pct,
coll_days_after_deal_feature,
coll_days_after_deal_close,
actual_quantity,
refund_quantity,
card_sale_amount,
credit_sale_amount,
reward_sale_amount,
card_refund_amount,
credit_refund_amount,
reward_refund_amount,
discount_sale_amount,
discount_refund_amount,
shipping_sale_amount,
shipping_refund_amount,
sales_tax_amount,
sales_tax_refund_amount,
booking_amount,
cogs_amount,
booked_cogs_amount,
revenue_amount,
gross_billing_amount,
profit_amount,
net_revenue_amount,
booked_net_revenue_amount,
gross_revenue_amount,
booking_quantity,
refund_amount,
old_total_resignation_amount,
total_resignation_amount,
card_resignation_amount,
credit_resignation_amount,
reward_resignation_amount,
discount_resignation_amount,
card_preterm_refund_amount,
credit_preterm_refund_amount,
rewards_preterm_refund_amount,
discount_preterm_refund_amount,
old_refund_less_resig_amt,
refunds_less_resignation_amt,
total_resignation_quantity,
card_resignation_quantity,
credit_resignation_quantity,
reward_resignation_quantity,
discount_resignation_quantity,
card_preterm_refund_quantity,
credit_preterm_refund_quantity,
rewards_preterm_refund_qty,
discount_preterm_refund_qty,
refunds_less_resignation_qty,
post_expiration_refund_amount,
post_expiration_refund_qty,
post_term_refund_amount,
post_term_refund_qty,
vip_deal_status_key,
src_modified_date,
status,
refund_reason_key,
medium,
lead_identifier,
ad_campaign_id,
refund_by_cs_flag,
est_3pl_per_unit_shipping_cost,
refund_allowance,
freight_allowance,
co_op,
est_per_unit_shipping_handling,
sales_tax_resignation_amount,
sales_tax_resignation_quantity,
checkin_date,
collection_date_key ds
from (
SELECT    c.collection_id , 
          c.collection_date_key , 
          c.order_id , 
          c.order_date_key , 
          c.order_created_date , 
          c.user_key , 
          c.deal_option_key , 
          dl.deal_key , 
          COALESCE( c.division_key , - 1 ) AS division_key , 
          COALESCE( c.domain_key ,   - 1 ) AS domain_key , 
          c.merchant_key , 
          c.country_key , 
          c.lead_parameters_key , 
          c.is_gift , 
          c.payment_type , 
          c.action , 
          c.user_source_key , 
          c.order_lead_id , 
          hour(c.src_created_date) AS order_hour, 
          ord.unit_buy_price                                      AS cost_to_groupon , 
          ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) AS cost_to_user , 
          c.point_category , 
          c.discount_key , 
          o.credit_card_fee_pct , 
         datediff(from_unixtime(unix_timestamp(c.collection_date_key ,'yyyyMMdd'), 'yyyy-MM-dd'),from_unixtime(unix_timestamp(dl.actual_start_date_key ,'yyyyMMdd'), 'yyyy-MM-dd')) AS coll_days_after_deal_feature , 
         datediff(from_unixtime(unix_timestamp(c.collection_date_key ,'yyyyMMdd'), 'yyyy-MM-dd'),from_unixtime(unix_timestamp(dl.actual_end_date_key ,'yyyyMMdd'), 'yyyy-MM-dd')) AS coll_days_after_deal_close , 
          CASE 
                    WHEN ord.unit_sell_price = 0 THEN 0.00 
                    WHEN c.payment_type = 'shipping' THEN 0.00 
                    ELSE ( ( 
                              CASE 
                                        WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN -c.amount
                                        ELSE c.amount 
                              END ) / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0.00 ) ) )
          END AS actual_quantity , 
          CASE 
                    WHEN ord.unit_sell_price = 0 THEN 0.00 
                    WHEN c.payment_type = 'shipping' THEN 0.00 
                    ELSE ( ( 
                              CASE 
                                        WHEN c.payment_type = 'discount' 
                                        AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 )
                                        WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' 
                                        AND       c.action = 'refund' THEN COALESCE( - c.amount , 0 )
                                        ELSE COALESCE( c.refund_amount , 0 ) 
                              END ) / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) )
          END AS refund_quantity , 
          CASE 
                    WHEN c.payment_type = 'creditcard' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END AS card_sale_amount , 
          CASE 
                    WHEN c.payment_type = 'credits' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END AS credit_sale_amount , 
          CASE 
                    WHEN c.payment_type = 'rewards' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END AS reward_sale_amount , 
          CASE 
                    WHEN c.payment_type = 'creditcard' THEN COALESCE( c.refund_amount , 0 ) 
                    ELSE 0.00 
          END AS card_refund_amount , 
          CASE 
                    WHEN c.payment_type = 'credits' THEN COALESCE( c.refund_amount , 0 ) 
                    ELSE 0.00 
          END AS credit_refund_amount , 
          CASE 
                    WHEN c.payment_type = 'rewards' THEN COALESCE( c.refund_amount , 0 ) 
                    ELSE 0.00 
          END AS reward_refund_amount , 
          CASE 
                    WHEN c.payment_type = 'discount' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END AS discount_sale_amount , 
          CASE 
                    WHEN c.payment_type = 'discount' 
                    AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 ) 
                    ELSE 0.00 
          END AS discount_refund_amount , 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END AS shipping_sale_amount , 
          CASE 
                    WHEN c.payment_type = 'shipping' 
                    AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 ) 
                    ELSE 0.00 
          END AS shipping_refund_amount , 
          COALESCE( 
          CASE 
                    WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( c.sale_amount , 0 )
                    WHEN substr ( c.payment_type , 1 , 11 ) = 'booking fee' THEN COALESCE( - c.sale_amount , 0 )
                    ELSE 0.00 
          END , 0 ) AS sales_tax_amount , 
          COALESCE( 
          CASE 
                    WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' 
                    AND       c.action = 'refund' THEN COALESCE( c.amount , 0 ) 
                    WHEN substr ( c.payment_type , 1 , 11 ) = 'booking fee' 
                    AND       c.action = 'refund' THEN COALESCE( - c.sale_amount , 0 ) 
                    ELSE 0.00 
          END , 0 ) AS sales_tax_refund_amount , 
          COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN 0.00 
                    WHEN substr ( c.payment_type , 1 , 12 ) = 'sales tax on' THEN COALESCE( - c.sale_amount , 0 )
                    WHEN c.payment_type = 'sales tax for getaways' THEN COALESCE(           - c.sale_amount , 0 )
                    ELSE COALESCE( c.sale_amount , 0 ) 
          END , 0 ) AS booking_amount , 
          ( COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN - c.sale_amount 
                    ELSE c.sale_amount 
          END , 0 ) + 
          CASE 
  WHEN c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','') THEN (
                              CASE 
                                        WHEN c.payment_type = 'discount' 
                                        AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 )
                                        ELSE COALESCE( 
                                                  CASE 
                                                            WHEN c.payment_type = 'shipping' THEN - c.refund_amount
                                                            ELSE c.refund_amount 
                                                  END , 0 ) 
                              END ) 
                    ELSE 0.00 
          END ) * ( 
          CASE 
                    WHEN ord.unit_sell_price = 0 THEN 1.00 
                    ELSE COALESCE( ( ord.unit_buy_price / ord.unit_sell_price ) , 0 ) 
          END ) AS cogs_amount , 
          ( COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN 0.00 
                    WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( - c.sale_amount , 0 )
                    ELSE c.sale_amount 
          END , 0 ) ) * ( 
          CASE 
                    WHEN ord.unit_sell_price = 0 THEN 1.00 
                    ELSE COALESCE( ( ord.unit_buy_price / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) ) , 0 )
          END ) AS booked_cogs_amount , 
          COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN - c.amount 
                    ELSE c.sale_amount 
          END , 0 ) + 
          CASE 
                    WHEN c.payment_type = 'creditcard' THEN COALESCE( c.refund_amount , 0 ) 
                    ELSE 0.00 
          END - 
          CASE 
                    WHEN c.payment_type = 'rewards' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END - 
          CASE 
                    WHEN c.payment_type = 'credits' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END - 
          CASE 
                    WHEN c.payment_type = 'discount' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END AS revenue_amount , 
          COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN - c.sale_amount 
                    ELSE c.sale_amount 
          END , 0 ) + 
          CASE 
          WHEN c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','') THEN (
                              CASE 
                                        WHEN c.payment_type = 'discount' 
                                        AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 )
                                        ELSE COALESCE( 
                                                  CASE 
                                                            WHEN c.payment_type = 'shipping' THEN - c.refund_amount
                                                            ELSE c.refund_amount 
                                                  END , 0 ) 
                              END ) 
                    ELSE 0.00 
          END AS gross_billing_amount , 
          ( COALESCE( c.sale_amount , 0 ) + 
          CASE 
                    WHEN c.payment_type = 'creditcard' THEN COALESCE( c.refund_amount , 0 ) 
                    ELSE 0.00 
          END - 
          CASE 
                    WHEN c.payment_type = 'rewards' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END - 
          CASE 
                    WHEN c.payment_type = 'credits' THEN COALESCE( c.sale_amount , 0 ) 
                    ELSE 0.00 
          END ) - ( ( COALESCE( c.sale_amount , 0 ) + ( 
          CASE 
                    WHEN c.payment_type = 'discount' 
                    AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 ) 
                    ELSE COALESCE( c.refund_amount , 0 ) 
          END ) ) * ( 
          CASE 
                    WHEN ord.unit_sell_price = 0 THEN -1.00 
                    ELSE COALESCE( ( ord.unit_buy_price / ord.unit_sell_price ) , 0 ) 
          END ) ) AS profit_amount , 
          ( COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN - c.sale_amount 
                    ELSE c.sale_amount 
          END , 0 ) + 
          CASE 
          WHEN c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','') THEN (
                              CASE 
                                        WHEN c.payment_type = 'discount' 
                                        AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 )
                                        ELSE COALESCE( c.refund_amount , 0 ) 
                              END ) 
                    ELSE 0.00 
          END ) - ( ( COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN - c.sale_amount 
                    ELSE c.sale_amount 
          END , 0 ) + 
          CASE 
WHEN c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','') THEN (
                              CASE 
                                        WHEN c.payment_type = 'discount' 
                                        AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 )
                                        ELSE COALESCE( c.refund_amount , 0 ) 
                              END ) 
                    ELSE 0.00 
          END ) * ( 
          CASE 
                    WHEN ord.unit_sell_price = 0 THEN -1.00 
                    ELSE COALESCE( ( ord.unit_buy_price / ord.unit_sell_price ) , 0 ) 
          END ) ) AS net_revenue_amount , 
          ( COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN COALESCE(                     - c.sale_amount , - c.amount , 0 )
                    WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( - c.sale_amount , 0 )
                    WHEN substr ( c.payment_type , 1 , 11 ) = 'booking fee' THEN COALESCE( c.sale_amount , 0 )
                    ELSE c.sale_amount 
          END , 0 ) ) - ( ( COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN COALESCE(                     - c.sale_amount , - c.amount , 0 )
                    WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( - c.sale_amount , 0 )
                    WHEN substr ( c.payment_type , 1 , 11 ) = 'booking fee' THEN 0.00 
                    ELSE c.sale_amount 
          END , 0 ) ) * ( 
          CASE 
                    WHEN ord.unit_sell_price = 0 THEN -1.00 
                    ELSE COALESCE( ( ord.unit_buy_price / ord.unit_sell_price ) , 0 ) 
          END ) ) + (CASE WHEN c.payment_type = 'shipping' THEN COALESCE( c.sale_amount , 0 ) ELSE 0.00 END) + (CASE  WHEN c.payment_type = 'shipping' AND c.action = 'cancel' THEN COALESCE( c.amount , 0 ) ELSE 0.00 END) AS booked_net_revenue_amount , 
          ( COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN 0.00 
                    WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( - c.sale_amount , 0 )
                    ELSE c.sale_amount 
          END , 0 ) ) - ( ( COALESCE( 
          CASE 
                    WHEN c.payment_type = 'shipping' THEN 0.00 
                    WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( - c.sale_amount , 0 )
                    ELSE c.sale_amount 
          END , 0 ) ) * ( 
          CASE 
                    WHEN ord.unit_sell_price = 0 THEN -1.00 
                    ELSE COALESCE( ( ord.unit_buy_price / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) ) , 0 )
          END ) ) AS gross_revenue_amount , 
          COALESCE( 
          CASE 
                    WHEN ord.unit_sell_price = 0 THEN 0.00 
                    WHEN c.payment_type = 'shipping' THEN 0.00 
                    WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN ( - c.amount / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) )
                    ELSE ( c.amount                                             / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) )
          END , 0 ) AS booking_quantity , ( 
          CASE 
                    WHEN c.payment_type = 'discount' 
                    AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 ) 
                    ELSE ( 
                              CASE 
                                        WHEN ( 
                                                            c.payment_type = 'shipping' ) THEN 0.00 
                                        WHEN substr ( c.payment_type , 1 , 12 ) = 'sales tax on' THEN COALESCE( - c.refund_amount , 0 )
                                        WHEN c.payment_type = 'sales tax for getaways' THEN COALESCE(           - c.refund_amount , 0 )
                                        ELSE COALESCE( c.refund_amount , 0 ) 
                              END ) 
          END ) AS refund_amount , ( 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'discount' 
                              AND       c.action = 'cancel' 
                              AND       ( 
                                                  c.collection_date_key <= COALESCE( dl.actual_end_date_key , 30000101 ) ) ) THEN COALESCE( c.amount , 0 )
                    ELSE 0.00 
          END + 
          CASE 
                    WHEN ( 
                                        c.payment_type IN ( 'creditcard' , 
                                                           'credits' , 
                                                           'rewards' , 
                                                           'sales tax on items' , 
                                                           'sales tax on shipping' , 
                                                           'sales tax for getaways' ) 
                              AND       ( 
                                                  c.collection_date_key <= COALESCE( dl.actual_end_date_key , 30000101 ) ) ) THEN
                              CASE 
                                        WHEN ( 
                                                            c.payment_type = 'shipping' ) THEN 0.00 
                                        WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( - c.refund_amount , 0 )
                                        ELSE COALESCE( c.refund_amount , 0 ) 
                              END 
                    ELSE 0.00 
          END ) AS old_total_resignation_amount , ( 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'discount' 
                              AND       c.action = 'cancel' 
                              AND       ( 
                                                  c.collection_date_key = ord.order_date_key ) ) THEN COALESCE( c.amount , 0 )
                    ELSE 0.00 
          END + 
          CASE 
                    WHEN ( 
                                        c.payment_type IN ( 'creditcard' , 
                                                           'credits' , 
                                                           'rewards' , 
                                                           'sales tax on items' , 
                                                           'sales tax on shipping' , 
                                                           'sales tax for getaways' , 
                                                           'booking fee for getaways' ) 
                              AND       ( 
                                                  c.collection_date_key = ord.order_date_key ) ) THEN
                              CASE 
                                        WHEN ( 
                                                            c.payment_type = 'shipping' ) THEN 0.00 
                                        WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( -1*c.refund_amount , 0 )
                                        ELSE COALESCE( c.refund_amount , 0 ) 
                              END 
                    ELSE 0.00 
          END ) AS total_resignation_amount , 
          0.00     AS card_resignation_amount , 
          0.00     AS credit_resignation_amount , 
          0.00     AS reward_resignation_amount , 
          0.00     AS discount_resignation_amount , 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'creditcard' 
AND   c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','') ) THEN COALESCE( c.refund_amount , 0 )
                    ELSE 0.00 
          END AS card_preterm_refund_amount , 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'credits' 
AND   c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','') ) THEN COALESCE( c.refund_amount , 0 )
                    ELSE 0.00 
          END AS credit_preterm_refund_amount , 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'rewards' 
AND   c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','') ) THEN COALESCE( c.refund_amount , 0 )
                    ELSE 0.00 
          END AS rewards_preterm_refund_amount , 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'discount' 
                              AND       c.action = 'cancel' 
AND   c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','') ) THEN COALESCE( c.refund_amount , 0 )
                    ELSE 0.00 
          END                                                 AS discount_preterm_refund_amount ,          
  ( (CASE WHEN c.payment_type = 'discount' AND c.action = 'cancel' THEN COALESCE( c.amount , 0 ) 
                    ELSE ( CASE WHEN ( c.payment_type = 'shipping' ) THEN 0.00 
                                WHEN substr ( c.payment_type , 1 , 12 ) = 'sales tax on' THEN COALESCE( -c.refund_amount , 0 )
                                WHEN c.payment_type = 'sales tax for getaways' THEN COALESCE( -c.refund_amount , 0 )
                                ELSE COALESCE( c.refund_amount , 0 ) 
                            END ) 
          END) - (CASE WHEN ( c.payment_type = 'discount' AND c.action = 'cancel' AND ( c.collection_date_key <= COALESCE( dl.actual_end_date_key , 30000101 ) ) ) THEN COALESCE( c.amount , 0 )
      ELSE 0.00 END + CASE WHEN ( c.payment_type IN ( 'creditcard' , 'credits' , 'rewards' , 'sales tax on items' , 'sales tax on shipping' , 'sales tax for getaways' ) 
      AND ( c.collection_date_key <= COALESCE( dl.actual_end_date_key , 30000101 ) ) ) THEN
CASE WHEN ( c.payment_type = 'shipping' ) THEN 0.00 
 WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( - c.refund_amount , 0 )
ELSE COALESCE( c.refund_amount , 0 ) 
END 
ELSE 0.00 
END ) ) AS old_refund_less_resig_amt , 
          ( (CASE WHEN c.payment_type = 'discount' AND c.action = 'cancel' THEN COALESCE( c.amount , 0 ) 
                    ELSE ( CASE WHEN ( c.payment_type = 'shipping' ) THEN 0.00 
                                WHEN substr ( c.payment_type , 1 , 12 ) = 'sales tax on' THEN COALESCE( -c.refund_amount , 0 )
                                WHEN c.payment_type = 'sales tax for getaways' THEN COALESCE( -c.refund_amount , 0 )
                                ELSE COALESCE( c.refund_amount , 0 ) 
                            END ) 
          END) - (CASE WHEN ( c.payment_type = 'discount' AND c.action = 'cancel' AND ( c.collection_date_key = ord.order_date_key ) ) THEN COALESCE( c.amount , 0 )
ELSE 0.00 END + CASE WHEN ( c.payment_type IN ( 'creditcard' ,'credits' ,'rewards' , 'sales tax on items' ,'sales tax on shipping' ,'sales tax for getaways' ,'booking fee for getaways' ) 
AND ( c.collection_date_key = ord.order_date_key ) ) THEN
CASE WHEN ( c.payment_type = 'shipping' ) THEN 0.00 WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( - c.refund_amount , 0 )
ELSE COALESCE( c.refund_amount , 0 ) END 
ELSE 0.00 
END)  )     AS refunds_less_resignation_amt , 
          0                                                   AS total_resignation_quantity , 
          0                                                   AS card_resignation_quantity , 
          0                                                   AS credit_resignation_quantity , 
          0                                                   AS reward_resignation_quantity , 
          0                                                   AS discount_resignation_quantity , 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'creditcard' 
AND   c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','')) THEN COALESCE(
                              CASE 
                                        WHEN ord.unit_sell_price = 0 THEN 0.00 
                                        WHEN c.payment_type = 'shipping' THEN 0.00 
                                        ELSE ( c.refund_amount / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) )
                              END , 0 ) 
                    ELSE 0.00 
          END AS card_preterm_refund_quantity , 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'credits' 
AND   c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','')) THEN COALESCE(
                              CASE 
                                        WHEN ord.unit_sell_price = 0 THEN 0.00 
                                        WHEN c.payment_type = 'shipping' THEN 0.00 
                                        ELSE ( c.refund_amount / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) )
                              END , 0 ) 
                    ELSE 0.00 
          END AS credit_preterm_refund_quantity , 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'rewards' 
AND   c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','')) THEN COALESCE(
                              CASE 
                                        WHEN ord.unit_sell_price = 0 THEN 0.00 
                                        WHEN c.payment_type = 'shipping' THEN 0.00 
                                        ELSE ( c.refund_amount / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) )
                              END , 0 ) 
                    ELSE 0.00 
          END AS rewards_preterm_refund_qty , 
          CASE 
                    WHEN ( 
                                        c.payment_type = 'discount' 
                              AND       c.action = 'cancel' 
AND   c.collection_date_key <=  regexp_replace(date_add(from_unixtime(unix_timestamp(COALESCE( dl.actual_end_date_key , 30000101 ) ,'yyyyMMdd'), 'yyyy-MM-dd'), 60),'-','')) THEN COALESCE(
                              CASE 
                                        WHEN ord.unit_sell_price = 0 THEN 0.00 
                                        WHEN c.payment_type = 'shipping' THEN 0.00 
                                        ELSE ( c.amount / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) )
                              END , 0 ) 
                    ELSE 0.00 
          END AS discount_preterm_refund_qty , 
          0   AS refunds_less_resignation_qty , 
          CASE 
  WHEN from_unixtime(unix_timestamp(c.collection_date_key ,'yyyyMMdd'), 'yyyy-MM-dd') > opt.expires_at THEN ( 
                              CASE 
                                        WHEN c.payment_type = 'discount' 
                                        AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 )
                                        ELSE COALESCE( c.refund_amount , 0 ) 
                              END ) 
                    ELSE 0.00 
          END AS post_expiration_refund_amount , 
          CASE 
  WHEN from_unixtime(unix_timestamp(c.collection_date_key ,'yyyyMMdd'), 'yyyy-MM-dd') > opt.expires_at THEN ( 
                              CASE 
                                        WHEN ord.unit_sell_price = 0 THEN 0.00 
                                        WHEN c.payment_type = 'shipping' THEN 0.00 
                                        ELSE ( ( 
                                                  CASE 
                                                            WHEN c.payment_type = 'discount' 
                                                            AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 )
                                                            ELSE COALESCE( c.refund_amount , 0 ) 
                                                  END ) / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) )
                              END ) 
                    ELSE 0.00 
          END AS post_expiration_refund_qty , 
          CASE 
  WHEN from_unixtime(unix_timestamp(c.collection_date_key ,'yyyyMMdd'), 'yyyy-MM-dd') BETWEEN (date_add(from_unixtime(unix_timestamp(dl.actual_end_date_key ,'yyyyMMdd'), 'yyyy-MM-dd'), 61)) AND opt.expires_at THEN (
                              CASE 
                                        WHEN c.payment_type = 'discount' 
                                        AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 )
                                        ELSE COALESCE( c.refund_amount , 0 ) 
                              END ) 
                    ELSE 0.00 
          END AS post_term_refund_amount , 
          CASE 
  WHEN from_unixtime(unix_timestamp(c.collection_date_key ,'yyyyMMdd'), 'yyyy-MM-dd') BETWEEN (date_add(from_unixtime(unix_timestamp(dl.actual_end_date_key ,'yyyyMMdd'), 'yyyy-MM-dd'), 61)) AND opt.expires_at THEN (
                              CASE 
                                        WHEN ord.unit_sell_price = 0 THEN 0.00 
                                        WHEN c.payment_type = 'shipping' THEN 0.00 
                                        ELSE ( ( 
                                                  CASE 
                                                            WHEN c.payment_type = 'discount' 
                                                            AND       c.action = 'cancel' THEN COALESCE( c.amount , 0 )
                                                            ELSE COALESCE( c.refund_amount , 0 ) 
                                                  END ) / ( ord.unit_sell_price + COALESCE( opt.shipping_cost , 0 ) ) )
                              END ) 
                    ELSE 0.00 
          END AS post_term_refund_qty , 
          CASE 
                    WHEN c.vip_flag = 1 
                    AND       c.vip_early_flag = 1 THEN 1 
                    WHEN c.vip_flag = 1 
                    AND       c.vip_extended_flag = 1 THEN 2 
                    WHEN c.vip_flag = 1 THEN 3 
                    ELSE - 1 
          END AS vip_deal_status_key , 
          c.src_modified_date , 
          c.status , 
          c.refund_reason_key , 
          c.medium , 
          c.lead_identifier , 
          c.ad_campaign_id , 
          c.refund_by_cs_flag , 
          opt.est_3pl_per_unit_shipping_cost AS est_3pl_per_unit_shipping_cost, 
          o.refund_allowance , 
          o.freight_allowance , 
          o.co_op , 
          mul.shipping_handling AS est_per_unit_shipping_handling, 
          CASE 
                    WHEN c.collection_date_key = ord.order_date_key THEN 
                              CASE 
                                        WHEN ( 
                                                            c.payment_type = 'shipping' ) THEN 0.00 
                                        WHEN substr ( c.payment_type , 1 , 9 ) = 'sales tax' THEN COALESCE( - c.refund_amount , 0 )
                                        ELSE 0.00 
                              END 
                    ELSE 0.00 
          END AS sales_tax_resignation_amount , 
          0   AS sales_tax_resignation_quantity , 
          c.checkin_date 
FROM      user_groupondw.fact_collections c 
LEFT OUTER JOIN prod_groupondw.fact_orders ord 
ON        ord.order_id = c.order_id 
JOIN      prod_groupondw.dim_deal_option opt 
ON        c.deal_option_key = opt.deal_option_key 
JOIN      prod_groupondw.dim_deal dl 
ON        opt.deal_key = dl.deal_key 
JOIN      dw.v_dim_multi_deal mul 
ON        c.deal_option_key = mul.deal_option_key 
LEFT OUTER JOIN 
          ( 
                   SELECT   opportunity_id , 
                            max ( credit_card_fee_pct ) AS credit_card_fee_pct , 
                            max ( refund_allowance )    AS refund_allowance , 
                            max ( freight_allowance )   AS freight_allowance , 
                            max ( co_op )               AS co_op 
                   FROM     prod_groupondw.dim_opportunity 
                   GROUP BY opportunity_id ) o 
ON        o.opportunity_id = substr ( dl.opportunity_id , 1 , 15 )
) temp distribute BY collection_date_key; 
