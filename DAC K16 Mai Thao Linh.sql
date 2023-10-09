-- Query 1
SELECT FORMAT_DATE ( '%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      sum (totals.visits) as visits,
      sum (totals.pageviews) as pageviews,
      sum (totals.transactions) as transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE FORMAT_DATE ( '%m',PARSE_DATE ('%Y%m%d', date) ) in ('01','02','03')
GROUP BY month
ORDER BY 1;

-- Query 2
select trafficSource.`source` as source,
      sum (totals.visits) as total_visits,
      sum (totals.bounces) as total_no_of_bounces,
      sum (totals.bounces)*100/sum (totals.visits) as bounce_rate 
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by source
order by 2 desc;
--correct

-- Query 3
with month as ( -- tính revenue Month --tính month thì ghi cte là month
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
week as ( -- -- tính revenue Week
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
--correct

-- Query 4
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

--câu 4 này lưu ý là mình nên dùng left join hoặc full join, bởi vì trong câu này, phạm vi chỉ từ tháng 6-7, nên chắc chắc sẽ có pur và nonpur của cả 2 tháng
--mình inner join thì vô tình nó sẽ ra đúng. nhưng nếu đề bài là 1 khoảng thời gian dài hơn, 2-3 năm chẳng hạn, nó cũng tháng chỉ có nonpur mà k có pur
--thì khi đó inner join nó sẽ làm mình bị mất data, thay vì hiện số của nonpur và pur thì nó để trống

--Query 5
select FORMAT_DATE ('%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  unnest (hits) hits,
  unnest (hits.product) product
where totals.transactions >= 1
      and product.productRevenue is not null
group by month
order by 1; 
--correct

--Query 6
select FORMAT_DATE ('%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      sum(product.productRevenue)/1000000/sum(totals.visits) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  unnest (hits) hits,
  unnest (hits.product) product
where totals.transactions is not null
      and product.productRevenue is not null
group by month
order by 1;
--correct

--Query 7
with buyer as (-- tìm ID người mua sp "YouTube Men's Vintage Henley"    --chỉnh lại tên cte
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
inner join buyer using (fullVisitorId)   --ở trên mình đã tìm ra tập buyer_list, thì xuống đây inner join là đc
where product.productRevenue is not null
      and product.v2ProductName != "YouTube Men's Vintage Henley"  
-- where fullVisitorId in (select ID from buyer)
--       and product.productRevenue is not null
--       and product.v2ProductName != "YouTube Men's Vintage Henley"
group by product.v2ProductName
order by 2 desc;

--chỉnh lại cách ghi 1 chút cho dễ nhìn hơn

--Query 8
with productview as (
select FORMAT_DATE ( '%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      count (eCommerceAction.action_type) as num_product_view
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  unnest (hits) hits,
  unnest (hits.product) product
where FORMAT_DATE ( '%m',PARSE_DATE ('%Y%m%d', date) ) in ('01','02','03')
      and eCommerceAction.action_type = '2'
      and product.productRevenue is null
group by month
order by 1
),
addtocart as (
select FORMAT_DATE ( '%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      count (eCommerceAction.action_type) as num_addtocart
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  unnest (hits) hits,
  unnest (hits.product) product
where FORMAT_DATE ( '%m',PARSE_DATE ('%Y%m%d', date) ) in ('01','02','03')
      and eCommerceAction.action_type = '3'
      and product.productRevenue is null
group by month
order by 1
),
purchaser as (
select FORMAT_DATE ( '%Y%m',PARSE_DATE ('%Y%m%d', date) ) as month,
      count (eCommerceAction.action_type) as num_purchase
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  unnest (hits) hits,
  unnest (hits.product) product
where FORMAT_DATE ( '%m',PARSE_DATE ('%Y%m%d', date) ) in ('01','02','03')
      and eCommerceAction.action_type = '6'
      and product.productRevenue is not null
group by month
order by 1
)
select pv.month,
      pv.num_product_view,
      a.num_addtocart,
      pc.num_purchase,
      round(a.num_addtocart*100/pv.num_product_view, 2) as add_to_cart_rate,
      round(pc.num_purchase*100/pv.num_product_view,2) as purchase_rate
from productview as pv
left join addtocart as a on pv.month=a.month
left join purchaser as pc on a.month=pc.month
order by 1;

--bài yêu cầu tính số sản phầm, mình nên count productName hay productSKU thì sẽ hợp lý hơn là count action_type
--k nên xài inner join, nếu table1 có 10 record,table2 có 5 record,table3 có 1 record, thì sau khi inner join, output chỉ ra 1 record

--Cách 1:dùng CTE
with
product_view as(
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

--bài này k nên inner join, vì nếu như bảng purchase k có data thì sẽ k mapping đc vs bảng productview, từ đó kết quả sẽ k có luôn, mình nên dùng left join

--Cách 2: bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;

                                                            ---very good---










