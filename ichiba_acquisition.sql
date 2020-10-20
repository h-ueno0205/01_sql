
--------------------------------------------------------------------------- Memo --
--- [1] Update raw data(fact and profile)

---[2] Run these queries
---	C:\work\05_analysis\200801_loyal_users\sql
---	00_insert_raw.sql
	--- [0] YM ID Categroy summary

---	01_mk_base.sql
	--- [1] YM ID Categroy summary
	
----------------------------------------------------------------------------------

---- base data
drop table sbx_rkm.ueno_icb_acq_rkm01;
create table sbx_rkm.ueno_icb_acq_rkm01 as (
select
	a.f_loyal,
	a.f_loyal_order,
	b.order_user_id,
	b.ym,
	c.cate_cd,
	c.cate,
	d.gender,
	d.birthday,
	d.easy_id,
	d.reg_datetime as rtn_reg_datetime,
	d.mem_status_cd,
	e.user_rank,
	b.cnt_order,
	b.gms
from
	(select * from sbx_rkm.ueno_loyal_buy01_base01 where ym between '2020/07' and '2020/09' ) as a
inner join
	sbx_rkm.ueno_loyal_buy01 as b on a.order_user_id = b.order_user_id and a.ym = b.ym
inner join
	sbx_rkm.ueno_loyal_cate01 as c on b.parent_id = c.cate_cd
left join
	(
		select
			d1.user_id,
			d1.gender,
			d1.birthday,
			d1.easy_id,
			d3.reg_datetime,
			d3.mem_status_cd
		from
			sbx_rkm.ueno_loyal_prf01 as d1
		inner join
			(select user_id, max(easy_id) as easy_id from sbx_rkm.ueno_loyal_prf01 group by 1) as d2 on d1.user_id = d2.user_id
		inner join
			ua_view_mk_id.red_member_tbl as d3 on d1.easy_id = d3.easy_id
	) as d on b.order_user_id = d.user_id
left join
	ua_view_mk_id.merge_user_rank as e on d.easy_id = e.easy_id and extract(year from cast(b.ym as date format 'YYYY/MM'))*100 + extract(month from cast(b.ym as date format 'YYYY/MM')) = e.rank_month
) with data
primary index(order_user_id, ym)
;
-- chk
select top 100 * from sbx_rkm.ueno_icb_acq_rkm01;
select top 100 * from UA_VIEW_MK_ICHIBA.shop_genre_master;


-- Ichiba data of Rakuma user
drop table sbx_rkm.ueno_icb_acq_ichiba01;
create table sbx_rkm.ueno_icb_acq_ichiba01 as (
	select
		service_detail_cd,
		order_no,
		basket_id,
		reg_datetime,
		easy_id,
		genre_id,
		shop_id,
		item_id,
		price,
		price_exc_tax,
		units,
		sub_total_amt,
		sub_total_amt_exc_tax,
		cancel_datetime
	from 
		UA_VIEW_MK_ICHIBA.red_basket_detail_tbl as a
	where
		service_detail_cd = 101 
		and 
		reg_datetime(date) between '2020-07-01' and '2020-09-30' 
		and
		cancel_datetime is null
		and
		exists(select distinct easy_id from sbx_rkm.ueno_icb_acq_rkm01 as b where a.easy_id = b.easy_id )
) with data
primary index(order_no, basket_id)
;

select top 100 * from sbx_rkm.ueno_icb_acq_ichiba01;
select count(*) from sbx_rkm.ueno_icb_acq_ichiba01;


-- Rakuma summary1
select
	b.g1,
	b.gn1,
	b.genre_id,
	b.genre_name,
	b.genre_name_all,
	case 
		when m_loyal = 1 then '01_loyal' 
		else '02_reg'
	end as f_loyal,
	case 
		when m_rnk in (5,6,7) then '01_DPG' 
		else '02_reg'
	end as rakuten_rank,
	count(distinct a.easy_id) as uu,
	count(distinct a.order_no) as frq,
	sum(cast(a.sub_total_amt_exc_tax as bigint)) as amt,
	rank() over(partition by f_loyal, rakuten_rank order by uu desc) as ranking 
from
	sbx_rkm.ueno_icb_acq_ichiba01 as a
inner join
	UA_VIEW_MK_ICHIBA.item_genre_dimension as b on a.genre_id = b.genre_id
inner join
	(
		select 
			easy_id,
			max(case when f_loyal in ('01_loyal') then 1 else 0 end) as m_loyal, 
			max(user_rank) as m_rnk 
		from 
			sbx_rkm.ueno_icb_acq_rkm01
		group by
			1
	) as c on a.easy_id = c.easy_id
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5
QUALIFY rank() over(partition by f_loyal, rakuten_rank order by uu desc) <= 200
;

-- chk(Heavy and DPG)
select
	case when m_loyal = 1 then '01_loyal' else '02_reg' end as f_loyal,
	case when m_rnk in (5,6,7) then '01_DPG' else '02_reg' end as rakuten_rank,
	count(distinct easy_id) as uu
from
	(
		select 
			easy_id,
			max(case when f_loyal in ('01_loyal') then 1 else 0 end) as m_loyal, 
			max(user_rank) as m_rnk 
		from 
			sbx_rkm.ueno_icb_acq_rkm01
		group by
			1
	) as a
group by 1,2
order by 1,2
;

-- subtotal for sort
select
	b.g1,
	b.gn1,
	b.genre_id,
	b.genre_name,
	b.genre_name_all,
	'null' as f_loyal,
	'null' as rakuten_rank,
	count(distinct a.easy_id) as uu,
	count(distinct a.order_no) as frq,
	sum(cast(a.sub_total_amt_exc_tax as bigint)) as amt
from
	sbx_rkm.ueno_icb_acq_ichiba01 as a
inner join
	UA_VIEW_MK_ICHIBA.item_genre_dimension as b on a.genre_id = b.genre_id
inner join
	(
		select 
			easy_id,
			max(case when f_loyal in ('01_loyal') then 1 else 0 end) as m_loyal, 
			max(user_rank) as m_rnk 
		from 
			sbx_rkm.ueno_icb_acq_rkm01
		group by
			1
	) as c on a.easy_id = c.easy_id
group by 1,2,3,4,5
order by 1,2,3,4,5
;

-- Potential customers in Ichiba
select
	b.g1,
	b.gn1,
	b.genre_id,
	b.genre_name,
	b.genre_name_all,
	count(distinct a.easy_id) as uu,
	count(distinct a.order_no) as frq,
	sum(cast(a.sub_total_amt_exc_tax as bigint)) as amt
from 
	UA_VIEW_MK_ICHIBA.red_basket_detail_tbl as a
inner join
	UA_VIEW_MK_ICHIBA.item_genre_dimension as b on a.genre_id = b.genre_id
where
	a.service_detail_cd = 101 
	and 
	a.reg_datetime(date) between '2020-07-01' and '2020-09-30' 
	and
	a.cancel_datetime is null
group by 1,2,3,4,5
order by 1,2,3,4,5
;

