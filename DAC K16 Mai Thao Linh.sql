-- Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT FORMAT_DATE ( '%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      sum (totals.visits) as visits,
      sum (totals.pageviews) as pageviews,
      sum (totals.transactions) as transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE FORMAT_DATE ( '%m',PARSE_DATE ('%Y%m%d', date) ) in ('01','02','03')
GROUP BY month
ORDER BY 1;


-- Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
select trafficSource.`source` as source,
      sum (totals.visits) as total_visits,
      sum (totals.bounces) as total_no_of_bounces,
      sum (totals.bounces)*100/sum (totals.visits) as bounce_rate 
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by source
order by 2 desc;


-- Query 3: Revenue by traffic source by week, by month in June 2017
with month as ( 
select 
      'Month' as time_type,
      FORMAT_DATE ( '%Y%m',PARSE_DATE ('%Y%m%d', date) ) as time,
      trafficSource.`source` as source,
      sum (productRevenue)/1000000 as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` ,
  unnest (hits) hits,
  unnest (hits.product) product
where productRevenue is not null
group by trafficSource.`source`, time
),
week as ( 
select 
      'Week' as time_type,
      FORMAT_DATE ( '%Y%W',PARSE_DATE ('%Y%m%d', date) ) as time,
      trafficSource.`source` as source,
      sum (productRevenue)/1000000 as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` ,
  unnest (hits) hits,
  unnest (hits.product) product
where productRevenue is not null
group by trafficSource.`source`, time   
)
select * from month
union all
select * from week
order by source, time_type, time;


-- Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
with purchaser as (
select FORMAT_DATE ('%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_purchase
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  unnest (hits) hits,
  unnest (hits.product) product
where _table_suffix between '0601' and '0731'
    and totals.transactions >= 1
    and product.productRevenue is not null
group by month
order by 1 
),
non_purchaser as (
select FORMAT_DATE ('%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_non_purchase
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  unnest (hits) hits,
  unnest (hits.product) product
where _table_suffix between '0601' and '0731'
    and totals.transactions is null
    and product.productRevenue is null
group by month
order by 1
)
select p.month,
        p.avg_pageviews_purchase,
        np.avg_pageviews_non_purchase
from purchaser as p
left join non_purchaser as np on p.month=np.month;


--Query 05: Average number of transactions per user that made a purchase in July 2017
select FORMAT_DATE ('%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  unnest (hits) hits,
  unnest (hits.product) product
where totals.transactions >= 1
      and product.productRevenue is not null
group by month
order by 1; 


--Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
select FORMAT_DATE ('%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      sum(product.productRevenue)/1000000/sum(totals.visits) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  unnest (hits) hits,
  unnest (hits.product) product
where totals.transactions is not null
      and product.productRevenue is not null
group by month
order by 1;


--Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
with buyer as (
      select distinct fullVisitorId --as ID
      from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
      unnest (hits) hits,
      unnest (hits.product) product
      where product.v2ProductName = "YouTube Men's Vintage Henley"
      and product.productRevenue is not null
)

select product.v2ProductName as other_purchased_products,
      sum(productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  unnest (hits) hits,
  unnest (hits.product) product
inner join buyer using (fullVisitorId)   
where product.productRevenue is not null
      and product.v2ProductName != "YouTube Men's Vintage Henley"  
group by product.v2ProductName
order by 2 desc;


--Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
with product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   --phải thêm điều kiện này để đảm bảo có revenue
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
left join add_to_cart a on pv.month = a.month
left join purchase p on pv.month = p.month
order by pv.month;











