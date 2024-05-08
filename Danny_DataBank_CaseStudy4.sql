-- A.Customer Nodes Exploration
-- 1.How many unique nodes are there on the Data Bank system?

select count(distinct(node_id)) as unique_node from customer_nodes;

-- 2.What is the number of nodes per region?

select rg.region_name ,count(cn.node_id) as nodes_count from regions rg
join customer_nodes cn on rg.region_id = cn.region_id
group by rg.region_name
order by count(cn.node_id) desc;

-- 3.How many customers are allocated to each region?

select rg.region_name ,count(distinct(cn.customer_id)) as customer_count from regions rg
join customer_nodes cn on rg.region_id = cn.region_id
group by rg.region_name
order by 2 desc;

-- 4.How many days on average are customers reallocated to a different node?

select round(avg((end_date - start_date))) as average_reallocation_days
from customer_nodes
where end_date <> '9999-12-31';

-- 5.What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

with reallocations_cte as (
select cn.customer_id,
cn.region_id,
rg.region_name,
(end_date - start_date) as reallocation_day
from customer_nodes cn 
join regions rg on rg.region_id = cn.region_id
where end_date <> '9999-12-31')

select region_name,
percentile_cont(0.5) within group(order by reallocation_day) as median_reallocation_days,
percentile_cont(0.8) within group(order by reallocation_day) as p80_reallocation_days,
percentile_cont(0.95) within group(order by reallocation_day) as p95_reallocation_days
from reallocations_cte
group by region_name;

-- B. Customer Transactions
-- 1.What is the unique count and total amount for each transaction type?

select txn_type,count(distinct(customer_id)) as unique_count, sum(txn_amount) as total_amount from customer_transactions
group by txn_type;

-- 2.What is the average total historical deposit counts and amounts for all customers?

with deposit_summary as (
  select customer_id ,count(1) as total_deposit_count,
  avg(txn_amount) as total_deposit_amount
  from customer_transactions
  where txn_type = 'deposit'
  group by customer_id
)

select round(avg(total_deposit_count)) as avg_deposit_count,
round(avg(total_deposit_amount)) as avg_deposit_amount from deposit_summary;

-- 3.For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?

with txntype_details as (
select customer_id,extract(month from txn_date) as month,
sum(case when txn_type = 'deposit' then 1 end) as total_deposit,
sum(case when txn_type = 'withdrawal' then 1 end) as total_withdrawal,
sum(case when txn_type = 'purchase' then 1 end) as total_purchase
from customer_transactions
group by customer_id,extract(month from txn_date))

select month,count(1) as customer_cnt
from txntype_details
where total_deposit > 1 and (total_withdrawal = 1 or total_purchase = 1)
group by month
order by count(1) desc;

-- 4.What is the closing balance for each customer at the end of the month?

with cte as (
select customer_id,
extract(month from txn_date) as month,
sum(case when txn_type = 'deposit' then txn_amount else -1*(txn_amount) end) as net_amount
from customer_transactions
group  by customer_id,extract(month from txn_date)
order by customer_id
)

select customer_id,
month,
net_amount,
sum(net_amount) over(partition by customer_id order by month) as closing_balance
from cte;

-- 5.What is the percentage of customers who increase their closing balance by more than 5%?

create view txn_details3 as
 (with cte1 as (
  select customer_id, extract(month from txn_date) as month,
   COALESCE(sum(case when txn_type = 'deposit' then txn_amount end), 0) as deposit,
   COALESCE(sum(case when txn_type = 'withdrawal' then (-1)*txn_amount end), 0) as withdrawal
  from customer_transactions
  group by 1,2
  order by 1),
 cte2 as (
  select *, (deposit + withdrawal) as total
  from cte1)
 select customer_id, month,
 sum(total) over(partition by customer_id order by customer_id,month rows between unbounded preceding and current row) as balance,
    total as change_in_balance
from cte2);
    
with cte1 as (
    select distinct customer_id,
        first_value(balance) over(partition by customer_id order by customer_id) as first_balance,
        last_value(balance) over(partition by customer_id order by customer_id) as last_balance
    from txn_details3
),
cte2 as (
    select *,
        round(((last_balance - first_balance) / first_balance) * 100, 2) as growth_rate
    from cte1
    where last_balance > first_balance
)
select round((count(*) * 100) / (select count(distinct customer_id) from customer_transactions), 2) as percent_customer
from cte2
where growth_rate >= 5;


-- C.Data Allocation Challenge

-- running customer balance column that includes the impact each transaction

with monthly_balances as ( 
 select customer_id, extract(month from txn_date) as month, 
 sum(case when txn_type='deposit' then txn_amount else -txn_amount end) as net_txn 
 from customer_transactions 
 group by customer_id,extract(month from txn_date)
),
runningbalance as ( 
 select customer_id,month,net_txn,
 sum(net_txn) over(partition by customer_id order by month rows between unbounded preceding and current row) as running_balance 
 from monthly_balances 
 group by customer_id,month,net_txn) 
    
select * from runningbalance

-- customer balance at the end of each month

with monthly_balances as ( 
 select customer_id, extract(month from txn_date) as month, 
 sum(case when txn_type='deposit' then txn_amount else -txn_amount end) as net_txn 
 from customer_transactions 
 group by customer_id,extract(month from txn_date)
),
runningbalance as ( 
 select customer_id,month,net_txn,
 sum(net_txn) over(partition by customer_id order by month rows between unbounded preceding and current row) as running_balance 
 from monthly_balances 
 group by customer_id,month,net_txn),

monthendbalance as (
 select *, last_value(running_balance) over(partition by customer_id order by month) as month_end_balance
 from runningbalance
 group by customer_id,month,net_txn,running_balance
    )

select customer_id,month,month_end_balance from monthendbalance

-- minimum, average and maximum values of the running balance for each customer

with monthly_balances as
(
  select customer_id, extract(month from txn_date) as month,
  sum(case when txn_type='deposit' then txn_amount else -txn_amount end) as net_txn
  from customer_transactions
  group by customer_id,extract(month from txn_date)
),
runningbalance as
(
 select customer_id,month,net_txn,
    sum(net_txn) over(partition by customer_id order by month rows between unbounded preceding and current row) as running_balance
    from monthly_balances
    group by customer_id,month,net_txn
)
select customer_id,min(running_balance) as min_balance,
max(running_balance) as max_balance,
round(avg(running_balance),2) as avg_balance
from runningbalance 
group by customer_id