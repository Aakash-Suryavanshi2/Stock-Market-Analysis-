/* 
Title: Buying stocks at a high price or any other price

Purpose: Analyze S&P stock market data from 1998-01-01 through 2020-08-28
	and look at the performance of purchases after 1yr, 3yr, and 5yr.

	Segment the data based on if someone where to make a purchase when the
	stock is trading at all-time-high, or if they were to make a purchase
	on any day.

	This is based on a report from JP MOrgan suggesting that you should buy
	at an all-time high and it's better than buying on any other day. Doesn't
	seem logical.
Goal: Understand if it makes more sense to purchase on days when the market
	is at an all-time high, vs waiting for the market to pull back. The prevaling
	advice is that you should wait for a pull-back and avoid bucking at an all-time
	high.
*/

/*
	Create table to load data
*/
create table sp_stock_date (
	trade_date date
	,close_price decimal(8, 2)
)

/*
	Create data params table to capture values that are going to be used throughout code
*/
drop table if exists data_params;
create temp table data_params as (
	select
		max(s.trade_date) - interval '1 year' as max_trade_dt_1yr_out
		,max(s.trade_date) - interval '3 year' as max_trade_dt_3yr_out
		,max(s.trade_date) - interval '5 year' as max_trade_dt_5yr_out
	from
		sp_stock_date s
)

/*
	Determine what the high-to-date price is for each date
*/
drop table if exists trade_w_high_price_to_date;
create temp table trade_w_high_price_to_date as (
	select 
		s.trade_date
		, s.close_price
		, max(s.close_price) over(order by s.trade_date rows between unbounded preceding and current row)as max_close_price_to_date
	from
		sp_stock_date s
	order by
		s.trade_date asc
)
;

/*
	Get the previous day high price for each trade to compare if the new price everyday is a high price
*/
drop table if exists trades_w_prev_day_high_price;
create temp table trades_w_prev_day_high_price as (
	select
		t.trade_date
		, t.close_price
		,t.max_close_price_to_date
		, lag(t.max_close_price_to_date) over (order by t.trade_date) as prev_day_max_close_price_to_date
	from
		trade_w_high_price_to_date t
	order by
		t.trade_date
)
;


/*
	Look at each record date and the current price, compare to the high-price-to-date
	and tag the record as being a new high or not being a new high
*/
drop table if exist trade_w_prev_day_high_price_n_flag;
create temp table trade_w_prev_day_high_price_n_flag as (
	select 
		t.trade_date
		, t.close_price
		, t.max_close_price_to_date
		, t.prev_day_max_close_price_to_date
		, case when (t.max_close_price_to_date = t.prev_day_max_close_price_to_date) then 'n' else 'y' end as is_new_high_yn
	from
		trades_w_prev_day_high_price t
	order by
		t.trade_date
)
;


/*
	Get the price of the stock 1yr, 3yr, 5yr, from the current date
*/
drop table if exists trades_w_dates_yrs_out;
create temp table trades_w_dates_yrs_out as (
	select 
		t.trade_date
		,t.close_price
		,t.max_close_price_to_date
		,t.prev_day_max_close_price_to_date
		,t.is_new_high_yn
		,(t.trade_date + interval '1 year') as trade_dt_plus_1yr
		,(t.trade_date + interval '3 year') as trade_dt_plus_3yr
		,(t.trade_date + interval '5 year') as trade_dt_plus_5yr
		, row_number() over (order by t.trade_date) as row_number
	from
		trade_w_prev_day_high_price_n_flag t
)

/*
	Column to find the nearest date to 1 year from current date with looking up to 10 days before
*/

drop table if exists trades_w_yr1_sales_date_row_num;
create temp table trades_w_yr1_sales_date_row_num as (
	select 
		t.trade_date
		,t.close_price
		,t.max_close_price_to_date
		,t.prev_day_max_close_price_to_date
		,t.is_new_high_yn
		,t.trade_dt_plus_1yr
		,t.trade_dt_plus_3yr
		,t.trade_dt_plus_5yr
		,t.row_number
		,max(yr1.row_number) as yr1_row_number
	from
		trades_w_dates_yrs_out t

		left join trades_w_dates_yrs_out yr1 
		on t.trade_dt_plus_1yr >= yr1.trade_date
					-- Gets the 10 days before the 1 year out incase the 1 year out date does not exist
				and (t.trade_dt_plus_1yr - interval '10 days') <= yr1.trade_date


	group by
		t.trade_date
		,t.close_price
		,t.max_close_price_to_date
		,t.prev_day_max_close_price_to_date
		,t.is_new_high_yn
		,t.trade_dt_plus_1yr
		,t.trade_dt_plus_3yr
		,t.trade_dt_plus_5yr
		,t.row_number
)

/*
	Column to find the nearest date to 3 year from current date with looking up to 10 days before
*/


drop table if exists trades_w_yr3_sales_date_row_num;
create temp table trades_w_yr3_sales_date_row_num as (
	select 
		t.trade_date
		,t.close_price
		,t.max_close_price_to_date
		,t.prev_day_max_close_price_to_date
		,t.is_new_high_yn
		,t.trade_dt_plus_1yr
		,t.trade_dt_plus_3yr
		,t.trade_dt_plus_5yr
		,t.row_number
		,t.yr1_row_number
		,max(yr3.row_number) as yr3_row_number
	from
		trades_w_yr1_sales_date_row_num t
		left join trades_w_yr1_sales_date_row_num yr3
		on t.trade_dt_plus_3yr >= yr3.trade_date
					-- Gets the 10 days before the 3 year out incase the 3 year out date does not exist
				and (t.trade_dt_plus_3yr - interval '10 days') <= yr3.trade_date
	group by
		t.trade_date
		,t.close_price
		,t.max_close_price_to_date
		,t.prev_day_max_close_price_to_date
		,t.is_new_high_yn
		,t.trade_dt_plus_1yr
		,t.trade_dt_plus_3yr
		,t.trade_dt_plus_5yr
		,t.row_number
		,t.yr1_row_number
)

/*
	Column to find the nearest date to 5 year from current date with looking up to 10 days before
*/

drop table if exists trades_w_yr1_3_5_sales_date_row_num;
create temp table trades_w_yr1_3_5_sales_date_row_num as (
	select 
		t.trade_date
		,t.close_price
		,t.max_close_price_to_date
		,t.prev_day_max_close_price_to_date
		,t.is_new_high_yn
		,t.trade_dt_plus_1yr
		,t.trade_dt_plus_3yr
		,t.trade_dt_plus_5yr
		,t.row_number
		,t.yr1_row_number
		,t.yr3_row_number
		,max(yr5.row_number) as yr5_row_number
	from
		trades_w_yr3_sales_date_row_num t
		left join trades_w_yr3_sales_date_row_num yr5
		on t.trade_dt_plus_5yr >= yr5.trade_date
					-- Gets the 10 days before the 5 year out incase the 5 year out date does not exist
				and (t.trade_dt_plus_5yr - interval '10 days') <= yr5.trade_date
	group by
		t.trade_date
		,t.close_price
		,t.max_close_price_to_date
		,t.prev_day_max_close_price_to_date
		,t.is_new_high_yn
		,t.trade_dt_plus_1yr
		,t.trade_dt_plus_3yr
		,t.trade_dt_plus_5yr
		,t.row_number
		,t.yr1_row_number
		,t.yr3_row_number
)

/*
	Take trades_w_perf and cleaned out values inaccurate values

	Find price closest to the target date

	Nulled out any values that havent occured yet since it will create false data 
	to fit the target date that doesnt always exist

	Created a data params table to capture values that will be used throughout the code.
*/

drop table if exists trades_w_return_perf;
create temp table trades_w_return_perf as (
	select 
		t.trade_date
		,t.close_price
		,t.max_close_price_to_date
		,t.prev_day_max_close_price_to_date
		,t.is_new_high_yn
		,t.trade_dt_plus_1yr
		,t.trade_dt_plus_3yr
		,t.trade_dt_plus_5yr
		,t.row_number
		,t.yr1_row_number
		,t.yr3_row_number
		,t.yr5_row_number
		
		--yr1 results
		, case when t.trade_date > (select d.max_trade_dt_1yr_out from data_params d)
				then null 
				else yr1.close_price 
		 	end as yr1_close_price

		,case when t.trade_date > (select d.max_trade_dt_1yr_out from data_params d)
				then null 
				else (yr1.close_price - t.close_price) 
			end as yr1_gain_dollars -- profit or loss after 1 year

		, case when t.trade_date > (select d.max_trade_dt_1yr_out from data_params d)
				then null 
				else round ((yr1.close_price - t.close_price) / t.close_price,4) 
			end as yr1_gain_percent

		--yr3 results
		, case when t.trade_date > (select d.max_trade_dt_3yr_out from data_params d)
				then null 
				else yr3.close_price 
		 	end as yr3_close_price

		,case when t.trade_date > (select d.max_trade_dt_3yr_out from data_params d)
				then null 
				else (yr3.close_price - t.close_price) 
			end as yr3_gain_dollars -- profit or loss after 1 year
			
		, case when t.trade_date > (select d.max_trade_dt_3yr_out from data_params d)
				then null 
				else round ((yr3.close_price - t.close_price) / t.close_price,4) 
			end as yr3_gain_percent

		--yr5 results
		, case when t.trade_date > (select d.max_trade_dt_5yr_out from data_params d)
				then null 
				else yr5.close_price 
		 	end as yr5_close_price

		,case when t.trade_date > (select d.max_trade_dt_5yr_out from data_params d)
				then null 
				else (yr5.close_price - t.close_price) 
			end as yr5_gain_dollars -- profit or loss after 1 year
			
		, case when t.trade_date > (select d.max_trade_dt_5yr_out from data_params d)
				then null 
				else round ((yr5.close_price - t.close_price) / t.close_price,4) 
			end as yr5_gain_percent

	from
		trades_w_yr1_3_5_sales_date_row_num t

			left join trades_w_yr1_3_5_sales_date_row_num yr1
				on t.yr1_row_number = yr1.row_number

			left join trades_w_yr1_3_5_sales_date_row_num yr3
				on t.yr3_row_number = yr3.row_number

			left join trades_w_yr1_3_5_sales_date_row_num yr5
				on t.yr5_row_number = yr5.row_number
	

	-- TODO For testing
	where
			1 = 1
		and t.trade_date >= '1998-01-01'
);


/*
Calculate the gains or losses for each trade date record in our dataset.
		(price_1yr_from_trade_date - price_at_trade_date)
*/

select 
	'all_trades' as segment
	,avg(t.yr1_gain_percent) as yr1_gain_perc
	,avg(t.yr3_gain_percent) as yr3_gain_perc
	,avg(t.yr5_gain_percent) as yr5_gain_perc
from 
	trades_w_return_perf t

union all

select 
	'new_high_trades' as segment
	,avg(t.yr1_gain_percent) as yr1_gain_perc
	,avg(t.yr3_gain_percent) as yr3_gain_perc
	,avg(t.yr5_gain_percent) as yr5_gain_perc
from
	trades_w_return_perf t
where
		1 = 1
	and 
		t.is_new_high_yn = 'y'


/*
	Summarize trade performance based on the year of the trade
*/
drop table if exists yearly_trade_performance
create temp table yearly_trade_performance as (
	select
		extract('year' from t.trade_date) as trade_year

		--year 1
		,avg(t.yr1_gain_percent) as all_trades_yr1_gain_perc
		,avg(case when t.is_new_high_yn = 'y' 
				then t.yr1_gain_percent 
				else null 
			end
		) as new_high_trades_yr1_gain_perc

		--year 3
		,avg(t.yr3_gain_percent) as all_trades_yr3_gain_perc
		,avg(case when t.is_new_high_yn = 'y' 
				then t.yr3_gain_percent 
				else null 
			end
		) as new_high_trades_yr3_gain_perc

		--year 5
		,avg(t.yr5_gain_percent) as all_trades_yr5_gain_perc
		,avg(case when t.is_new_high_yn = 'y' 
				then t.yr5_gain_percent 
				else null 
			end
		) as new_high_trades_yr5_gain_perc
	from
		trades_w_return_perf t
	group by
		trade_year
	order by
		trade_year

);


/*
	Median values for performance off trades for year 1
*/
drop table if exists yr1_gain_perc_medians;
create temp table yr1_gain_perc_medians as (
	select
		'all_trade' as segment
		,max(p.yr1_gain_percent) as yr1_median_gain

	from (
		select
			t.trade_date
			,t.yr1_gain_percent
			,percent_rank() over (order by t.yr1_gain_percent) as yr1_percentile
		from
			trades_w_return_perf t
		order by
			t.yr1_gain_percent
		) p
	where
			1 = 1
		and p.yr1_percentile <= 0.50
	group by
		segment

	union all

	select
		'new_high_trades' as segment
		,max(p.yr1_gain_percent) as yr1_median_gain

	from (
		select
			t.trade_date
			,t.yr1_gain_percent
			,percent_rank() over (order by t.yr1_gain_percent) as yr1_percentile
		from
			trades_w_return_perf t
		where
				1 = 1
			and t.is_new_high_yn = 'y'
		order by
			t.yr1_gain_percent
		) p
	where
			1 = 1
		and p.yr1_percentile <= 0.50
	group by
		segment
)
;


/*
	Median values for performance off trades for year 3
*/
drop table if exists yr3_gain_perc_medians;
create temp table yr3_gain_perc_medians as (
	select
		'all_trade' as segment
		,max(p.yr3_gain_percent) as yr3_median_gain

	from (
		select
			t.trade_date
			,t.yr3_gain_percent
			,percent_rank() over (order by t.yr3_gain_percent) as yr3_percentile
		from
			trades_w_return_perf t
		order by
			t.yr3_gain_percent
		) p
	where
			1 = 1
		and p.yr3_percentile <= 0.50
	group by
		segment

	union all

	select
		'new_high_trades' as segment
		,max(p.yr3_gain_percent) as yr3_median_gain

	from (
		select
			t.trade_date
			,t.yr3_gain_percent
			,percent_rank() over (order by t.yr3_gain_percent) as yr3_percentile
		from
			trades_w_return_perf t
		where
				1 = 1
			and t.is_new_high_yn = 'y'
		order by
			t.yr3_gain_percent
		) p
	where
			1 = 1
		and p.yr3_percentile <= 0.50
	group by
		segment
)
;


/*
	Median values for performance off trades for year 5
*/
drop table if exists yr5_gain_perc_medians;
create temp table yr5_gain_perc_medians as (
	select
		'all_trade' as segment
		,max(p.yr5_gain_percent) as yr5_median_gain

	from (
		select
			t.trade_date
			,t.yr5_gain_percent
			,percent_rank() over (order by t.yr5_gain_percent) as yr5_percentile
		from
			trades_w_return_perf t
		order by
			t.yr5_gain_percent
		) p
	where
			1 = 1
		and p.yr5_percentile <= 0.50
	group by
		segment

	union all

	select
		'new_high_trades' as segment
		,max(p.yr5_gain_percent) as yr5_median_gain

	from (
		select
			t.trade_date
			,t.yr5_gain_percent
			,percent_rank() over (order by t.yr5_gain_percent) as yr5_percentile
		from
			trades_w_return_perf t
		where
				1 = 1
			and t.is_new_high_yn = 'y'
		order by
			t.yr5_gain_percent
		) p
	where
			1 = 1
		and p.yr5_percentile <= 0.50
	group by
		segment
)
;

/*
	combine yrs 1,3,5 into a single result set
*/

select
	yr1.segment
	,yr1.yr1_median_gain
	,yr3.yr3_median_gain
	,yr5.yr5_median_gain

from
	yr1_gain_perc_medians yr1

		join yr3_gain_perc_medians yr3
			on yr1.segment = yr3.segment

		join yr5_gain_perc_medians yr5
			on yr1.segment = yr5.segment

order by
	yr1.segment
