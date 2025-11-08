




drop table if exists {TEMP_SCHEMA}.wj_campaign_engagement_master;

create table {TEMP_SCHEMA}.wj_campaign_engagement_master as
select distinct a0.loyaltymemberkey
    ,a0.external_id
    ,b0.subscriberkey
    ,b0.subscriberid
    ,a0.journey_type
    ,a0.ntj_entry_flag
    ,a0.wj_re_entry_flag
    ,se.first_touch
    ,created_date
    ,coalesce(a0.journey_join_date,a0.first_touch,se.first_touch) as journey_join_date
    ,case
        when i0.contact_id is not null then 0
        else 1
    end as is_in_welcome_journey
    ,case
        when k0.contact_id is not null then 1
        else 0
    end as is_offer2_holdout
    ,case
        when j0.contact_id is not null then 1
        else 0
    end as is_offer3_holdout
    ,b0.sendid
    ,b0.senttime
    ,b0.emailname
    ,b0.subject
    ,case
        when b0.sendid = g0.sendid
        and b0.subscriberid = g0.subscriberid then 1
        else 0
    end as isbounced
    ,bouncetype
    ,g0.bounce_timestamp
    ,c0.total_opens
    ,c0.first_open_timestamp
    ,c0.last_open_timestamp
    ,e0.total_clicks
    ,e0.first_click_timestamp
    ,e0.last_click_timestamp
    ,case
        when first_open_timestamp is null then null
        else datediff(hour, b0.senttime, first_open_timestamp)
    end as time_to_first_open
    ,case
        when first_click_timestamp is null then null
        else datediff(hour,first_open_timestamp,first_click_timestamp)
    end as time_to_1st_click_from_1st_open
    ,case
        when a0.subscriberid = h0.subscriberid then 1
        else 0
    end as has_unsubscribed
    ,case
        when a0.subscriberid = h0.subscriberid then unsub_time
        else null
    end as unsubscribed_timestamp
from /*(
        select distinct subscriberkey
            ,subscriberid
            ,b1.emailname
            ,b1.subject
            ,min(a1.sendid) as sendid
            ,min(eventdate) as senttime
        from {TARGET_SCHEMA}.sfmc_sent a1
            inner join (
                select *
                from {TARGET_SCHEMA}.sfmc_sendjobs
                where upper(emailname) like '%WJ%'
                and triggeredsendexternalkey is not null
            ) b1 ON a1.sendid = b1.sendid
        group by subscriberkey,subscriberid,b1.emailname,b1.subject
    ) b0*/
 (select distinct subscriberkey
            ,subscriberid
            ,emailname
            ,subject
            ,sendid
            ,senttime
	from
	(select distinct subscriberkey
            ,subscriberid
            ,emailname
            ,subject
            ,sendid
            ,senttime
            ,row_number() over (partition by subscriberkey, touch_name order by senttime) as rank1
           from
	(select distinct subscriberkey
            ,subscriberid
            ,b1.emailname
            ,b1.subject
            ,case when emailname LIKE 'WJ%1.1%' then 'WJ 1.1'
				 when emailname LIKE 'WJ%1.2%' then 'WJ 1.2'
				 when emailname LIKE 'WJ%1.3%' then 'WJ 1.3'
				 when emailname LIKE 'WJ%1.4%' then 'WJ 1.4'
				 when emailname LIKE 'WJ%2.1%' then 'WJ 2.1'
				 when emailname LIKE 'WJ%2.2%' then 'WJ 2.2'
				 when emailname LIKE 'WJ%2.3%' then 'WJ 2.3'
				 when emailname LIKE 'WJ%2.4%' then 'WJ 2.4'
				 when emailname LIKE 'WJ%2.5%' then 'WJ 2.5'
				 when emailname LIKE 'WJ%2.6%' then 'WJ 2.6'
				 when emailname LIKE 'WJ%3.1%' then 'WJ 3.1'
				 when emailname LIKE 'WJ%3.2%' then 'WJ 3.2'
				 when emailname like '%WJ%3.3%' then 'WJ 3.3'
				 when emailname like 'WJ%3.4%' then 'WJ 3.4'
				 when emailname like 'WJ%3.5%' then 'WJ 3.5'
				 when emailname like 'WJ%4.1%' then 'WJ 4.1'
				 when emailname LIKE 'WJ%4.2%' then 'WJ 4.2'
				 when emailname like 'WJ%4.4%' then 'WJ 4.4'
				 when emailname like 'WJ%4.3%' then 'WJ 4.3'
				 when emailname like '%NTJ%2.8%' then 'NTJ 2.8'
			end as touch_name
            ,min(a1.sendid) as sendid
            ,min(eventdate) as senttime
        from {TARGET_SCHEMA}.sfmc_sent a1
            inner join (
                select *
                from {TARGET_SCHEMA}.sfmc_sendjobs
                where (upper(emailname) like '%WJ%' or (upper(emailname) like '%WJ%' and emailname like '%2.8%')
				or (upper(emailname) like '%NTJ%' and emailname like '%2.8%'))
   				and upper(emailname) not like '%RWJ%'-- change
                and triggeredsendexternalkey is not null
            ) b1 
         ON a1.sendid = b1.sendid
        group by 1,2,3,4,5
        ))
        where rank1 = 1
    ) b0	
    left join (
        select contact_id as subscriberkey
            ,subscriberid
            ,loyaltymemberkey
            ,external_id
            ,first_touch
            ,journey_join_date
            ,created_date
            ,journey_type
            ,ntj_entry_flag
            ,wj_re_entry_flag
        from {TEMP_SCHEMA}.welcome_journey_loyaltymemberkey_contactid_mapping
    ) a0 ON a0.subscriberid = b0.subscriberid
    left join (
        select subscriberkey
            ,subscriberid
            ,sendid
            ,count(*) as total_opens
            ,min(eventdate) as first_open_timestamp
            ,max(eventdate) as last_open_timestamp
        from {TARGET_SCHEMA}.sfmc_opens
        group by subscriberkey,subscriberid,sendid
    ) c0 ON b0.subscriberid = c0.subscriberid
    and b0.sendid = c0.sendid
    left join (
        select subscriberkey
            ,subscriberid
            ,sendid
            ,count(*) as total_clicks
            ,min(eventdate) as first_click_timestamp
            ,max(eventdate) as last_click_timestamp
        from {TARGET_SCHEMA}.sfmc_clicks
        group by subscriberkey,subscriberid,sendid
    ) e0 ON b0.subscriberid = e0.subscriberid
    and b0.sendid = e0.sendid
    left join (
        select subscriberkey
            ,subscriberid
            ,sendid
            ,bouncecategory as bouncetype
            ,min(eventdate) as bounce_timestamp
        from {TARGET_SCHEMA}.sfmc_bounces
        group by subscriberkey,subscriberid,sendid,bouncecategory
    ) g0 ON b0.subscriberid = g0.subscriberid
    and b0.sendid = g0.sendid
    left join (
        select subscriberkey
            ,subscriberid
            ,sendid
            ,min(eventdate) as unsub_time
        from {TARGET_SCHEMA}.sfmc_unsubs
        group by subscriberkey,subscriberid,sendid
    ) h0 ON b0.subscriberid = h0.subscriberid
    and b0.sendid = h0.sendid
    left join (
        select distinct contact_id
        from (
                select distinct contact_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup_bkup
                where lower(controlgroup) in ('wj_cg')
                 and event_date is not null
                union all
                select distinct contact_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where lower(controlgroup) in ('wj_cg')
                 and control_event_date is not null
            )
    ) i0 ON b0.subscriberkey = i0.contact_id
    left join (
        select distinct contact_id
        from (
                select contact_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup_bkup
                where lower(controlgroup) in ('wj_offer_2_holdout')
                    and event_date is not null
                union all
                select contact_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where lower(controlgroup) in ('wj_offer_2_holdout')
                    and control_event_date is not null
                union all
                select contact_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where touch3_holdout_type is not null
            )
    ) k0 ON b0.subscriberkey = k0.contact_id
    left join (
        select distinct contact_id
        from (
                select contact_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup_bkup
                where lower(controlgroup) in ('wj_offer_3_holdout')
                    and event_date is not null
                union all
                select contact_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where lower(controlgroup) in ('wj_offer_3_holdout')
                    and control_event_date is not null
                union all
                select contact_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where touch4_holdout_type is not null
            )
    ) j0 ON b0.subscriberkey = j0.contact_id
    left join (
        select subscriberkey,
            subscriberid,
            min(eventdate) as first_touch
        from {TARGET_SCHEMA}.sfmc_sent
        where sendid in (
                select distinct sendid
                from {TARGET_SCHEMA}.sfmc_sendjobs
                where upper(emailname) like ('WJ%')
            ) 
        group by subscriberkey,
            subscriberid
    ) se 
   on b0.subscriberid = se.subscriberid
;

drop table if exists {TABLEAU_SCHEMA}.wj_campaign_engagement_master;
create table {TABLEAU_SCHEMA}.wj_campaign_engagement_master as
    select * from {TEMP_SCHEMA}.wj_campaign_engagement_master;

grant select on {TABLEAU_SCHEMA}.wj_campaign_engagement_master to public ;






create table {TEMP_SCHEMA}.welcome_journey_non_ntj_master distkey(loyaltymemberkey) compound sortkey(loyaltymemberkey) as
select distinct 
	a0.loyaltymemberkey,
    a0.external_id,
    a0.loyalty_member_id,
    a0.ntj_entry_date,
    a0.ntj_entry_flag,
    a0.wj_re_entry_date,
    a0.wj_re_entry_flag,
    a0.accountcreationdatetime,
    a0.signup_date,
    a0.created_date,
    a0.journey_join_date,
    a0.cust_tag,
    a0.first_touch,
    a0.journey_type,
    substring(created_date, 1, 4) || substring(created_date, 6, 2) as start_month_cohort,
    case
        when date_part(dow, created_date) = 0 then created_date
        else next_day(created_date, 'Sunday')
    end as journey_cohort_week,
    substring(dateadd(day, 90, created_date), 1, 4) || substring(dateadd(day, 90, created_date), 6, 2) as end_month_cohort,
    /*case
        when date_part(dow, dateadd(day, 90, created_date)) = 0 then dateadd(day, 90, created_date)
        else next_day(dateadd(day, 90, created_date), 'Sunday')
    end as journey_cohort_end_week,*/
    case
        when date_part(dow, case when j0._3rd_txn_date is not null then j0._3rd_txn_date else dateadd(day, 90, a0.created_date) end) = 0 
		then (case when j0._3rd_txn_date is not null then j0._3rd_txn_date else dateadd(day, 90, a0.created_date) end)
        else next_day(case when j0._3rd_txn_date is not null then j0._3rd_txn_date else dateadd(day, 90, a0.created_date) end, 'Sunday')
    end as journey_cohort_end_week,
    a0.subscriberid,
    a0.subscriberkey,
    case
        when z4.txns_before_wj > 0 then 1
        else 0
    end as have_txns_before_wj_flag,
    case
        when a0.signup_date < '2020-11-10' then 1
        else 0
    end oldcustomer_flag,
    case
        when a0.signup_date < '2020-11-10'
        or z4.txns_before_wj > 0 then 0
        else 1
    end as is_eligible,
    case
        when b0.external_id is not null then 0
        else 1
    end as is_in_welcome_journey,
    case
        when c0.external_id is not null then 1
        else 0
    end as is_offer2_holdout,
    case
        when d0.external_id is not null then 1
        else 0
    end as is_offer3_holdout,
    j0._1st_txn_date,
    j0._1st_txn_purchase_channel,
    j0._1st_txn_netsales,
    j0._2nd_txn_date,
    j0._2nd_txn_purchase_channel,
    j0._2nd_txn_netsales,
    j0._3rd_txn_date,
    j0._3rd_txn_purchase_channel,
    j0._3rd_txn_netsales,
    case
        when j0._1st_txn_saleschannelname in ('IN STORE') THEN 'INSTORE'
        when j0._1st_txn_saleschannelname in ('WHITELABEL', 'ORDER AHEAD','CATERING') THEN 'DIGITAL'
    end as _1st_txn_purchase_medium,
    case
        when j0._2nd_txn_saleschannelname in ('IN STORE') THEN 'INSTORE'
        when j0._2nd_txn_saleschannelname in ('WHITELABEL', 'ORDER AHEAD','CATERING') THEN 'DIGITAL'
    end as _2nd_txn_purchase_medium,
    case
        when j0._3rd_txn_saleschannelname in ('IN STORE') THEN 'INSTORE'
        when j0._3rd_txn_saleschannelname in ('WHITELABEL', 'ORDER AHEAD','CATERING') THEN 'DIGITAL'
    end as _3rd_txn_purchase_medium,
    case
        when j0._1st_txn_date is not null then datediff(day, j0.signup_date, j0._1st_txn_date)
    end as Days_to_1st_txn_since_signup,
    case
        when j0._2nd_txn_date is not null then datediff(day, j0.signup_date, j0._2nd_txn_date)
    end as Days_to_2nd_txn_since_signup,
    case
        when j0._3rd_txn_date is not null then datediff(day, j0.signup_date, j0._3rd_txn_date)
    end as Days_to_3rd_txn_since_signup,
    j0.days1_2_purchase,
    j0.days2_3_purchase,
    j1.total_sales_90_days,
    j1.total_txns_90_days,
    case
        when z1.offercatalogcampaignkey in ('6036', '2371', '6964', '7120') then 1
        else 0
    end as has_redeemed_offer1,
    --(2nd Change)
    case
        when z1.offercatalogcampaignkey in ('6036', '2371', '6964', '7120') then z1.redemptiondate
        else null
    end as redemption_date_offer1,
    -- (3rd Change)
    x0.offer_3_3_variants,
    case
        when x0.offer_3_3_variants = 'Choice of Side with Chips + 2x points multiplier on next purchase' then 'Offer2A'
        when x0.offer_3_3_variants = 'Free Fountain Drink + 2x points multiplier on the order' then 'Offer2B'
        when x0.offer_3_3_variants = '250 bonus points' then 'Offer2C'
        when x0.offer_3_3_variants = '300 Bonus points on order-ahead or delivery purchase' then 'Offer2D'
        when x0.offer_3_3_variants = 'Get $0 Delivery Fee' then 'Offer2C_2'
        when x0.offer_3_3_variants = 'Free Fountain Drink + 3x points multiplier on the order' then 'Offer2D_2'
        when x0.offer_3_3_variants = '2x points on next purchase' then 'Offer_2D'
        when x0.offer_3_3_variants = 'Free Fountain Drink' then 'Offer_2A1'
        when x0.offer_3_3_variants = 'Free Side and Chips' then 'Offer_2A2'
        when x0.offer_3_3_variants = 'Free Fountain Drink with Chips' then 'Non Digital Offer2C'
        when x0.offer_3_3_variants = '350 bonus points on click' then 'Offer_2F'
        when x0.offer_3_3_variants = '3x points on next purchase' then 'Offer_2D_3x'
        when x0.offer_3_3_variants = '150 bonus points on next purchase' then 'Offer_2E'
    end as offer_3_3_variants_short,
    case
        when x0.offer_3_3_variants = 'Choice of Side with Chips + 2x points multiplier on next purchase'
        and (
            z0.offercatalogcampaignkey = 6034
            or z2.loyaltymemberkey is not null
        ) then z2.name
        when x0.offer_3_3_variants = 'Free Fountain Drink + 2x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6035', '6085')
            or z2.loyaltymemberkey is not null
        ) then z2.name
        when x0.offer_3_3_variants = '250 bonus points'
        and z2.loyaltymemberkey is not null then z2.name
        when x0.offer_3_3_variants = '300 Bonus points on order-ahead or delivery purchase'
        and z2.loyaltymemberkey is not null then z2.name
        when x0.offer_3_3_variants = 'Get $0 Delivery Fee'
        and z0.offercatalogcampaignkey = 6371 then 'Offer2C_2'
        when x0.offer_3_3_variants = 'Free Fountain Drink + 3x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6368', '6367')
            or z2.loyaltymemberkey is not null
        ) then 'Offer2D_2'
        when x0.offer_3_3_variants = '2x points on next purchase'
        and z2.loyaltymemberkey is not null then z2.name
        when x0.offer_3_3_variants = '150 bonus points on next purchase'
        and z2.loyaltymemberkey is not null then z2.name
        when x0.offer_3_3_variants = 'Free Fountain Drink'
        and z0.offercatalogcampaignkey in ('7026') then 'Offer_2A1'
        when x0.offer_3_3_variants = 'Free Side and Chips'
        and z0.offercatalogcampaignkey in ('7027') then 'Offer_2A2'
        when x0.offer_3_3_variants = 'Free Fountain Drink with Chips'
        and z0.offercatalogcampaignkey in ('7032') then 'Non Digital Offer2C'
        when x0.offer_3_3_variants = '350 bonus points on click'
        and z2.loyaltymemberkey is not null then z2.name
        when x0.offer_3_3_variants = '3x points on next purchase'
        and z2.loyaltymemberkey is not null then z2.name
    end as offer_3_3_variant_redeemed,
    case
        when x0.offer_3_3_variants = 'Choice of Side with Chips + 2x points multiplier on next purchase'
        and (
            z0.offercatalogcampaignkey = 6034
            or z2.loyaltymemberkey is not null
        ) then 1
        when x0.offer_3_3_variants = 'Free Fountain Drink + 2x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6035', '6085')
            or z2.loyaltymemberkey is not null
        ) then 1
        when x0.offer_3_3_variants = '250 bonus points'
        and z2.loyaltymemberkey is not null then 1
        when x0.offer_3_3_variants = '300 Bonus points on order-ahead or delivery purchase'
        and z2.loyaltymemberkey is not null then 1
        when x0.offer_3_3_variants = 'Get $0 Delivery Fee'
        and z0.offercatalogcampaignkey = 6371 then 1
        when x0.offer_3_3_variants = 'Free Fountain Drink + 3x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6368', '6367')
            or z2.loyaltymemberkey is not null
        ) then 1
        when x0.offer_3_3_variants = '2x points on next purchase'
        and z2.loyaltymemberkey is not null then 1
        when x0.offer_3_3_variants = '150 bonus points on next purchase'
        and z2.loyaltymemberkey is not null then 1
        when x0.offer_3_3_variants = 'Free Fountain Drink'
        and z0.offercatalogcampaignkey in ('7026') then 1
        when x0.offer_3_3_variants = 'Free Side and Chips'
        and z0.offercatalogcampaignkey in ('7027') then 1
        when x0.offer_3_3_variants = 'Free Fountain Drink with Chips'
        and z0.offercatalogcampaignkey in ('7032') then 1
        when x0.offer_3_3_variants = '350 bonus points on click'
        and z2.loyaltymemberkey is not null then 1
        when x0.offer_3_3_variants = '3x points on next purchase'
        and z2.loyaltymemberkey is not null then 1
    end as has_redeemed_offer2,
    case
        when x0.offer_3_3_variants = 'Choice of Side with Chips + 2x points multiplier on next purchase'
        and (
            z0.offercatalogcampaignkey = 6034
            or z2.loyaltymemberkey is not null
        ) then coalesce(z0.redemptiondate, z2.redemptiondate)
        when x0.offer_3_3_variants = 'Free Fountain Drink + 2x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6035', '6085')
            or z2.loyaltymemberkey is not null
        ) then coalesce(z0.redemptiondate, z2.redemptiondate)
        when x0.offer_3_3_variants = '250 bonus points'
        and z2.loyaltymemberkey is not null then z2.redemptiondate
        when x0.offer_3_3_variants = '300 Bonus points on order-ahead or delivery purchase'
        and z2.loyaltymemberkey is not null then z2.redemptiondate
        when x0.offer_3_3_variants = 'Get $0 Delivery Fee'
        and z0.offercatalogcampaignkey = 6371 then z0.redemptiondate
        when x0.offer_3_3_variants = 'Free Fountain Drink + 3x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6368', '6367')
            or z2.loyaltymemberkey is not null
        ) then coalesce(z0.redemptiondate, z2.redemptiondate)
        when x0.offer_3_3_variants = '2x points on next purchase'
        and z2.loyaltymemberkey is not null then z2.redemptiondate
        when x0.offer_3_3_variants = '150 bonus points on next purchase'
        and z2.loyaltymemberkey is not null then z2.redemptiondate
        when x0.offer_3_3_variants = 'Free Fountain Drink'
        and z0.offercatalogcampaignkey in ('7026') then z0.redemptiondate
        when x0.offer_3_3_variants = 'Free Side and Chips'
        and z0.offercatalogcampaignkey in ('7027') then z0.redemptiondate
        when x0.offer_3_3_variants = 'Free Fountain Drink with Chips'
        and z0.offercatalogcampaignkey in ('7032') then z0.redemptiondate
        when x0.offer_3_3_variants = '350 bonus points on click'
        and z2.loyaltymemberkey is not null then z2.redemptiondate
        when x0.offer_3_3_variants = '3x points on next purchase'
        and z2.loyaltymemberkey is not null then z2.redemptiondate
    end as redemption_date_offer2,
    case
        when x0.offer_3_3_variants = 'Choice of Side with Chips + 2x points multiplier on next purchase'
        and (
            z0.offercatalogcampaignkey = 6034
            or z2.loyaltymemberkey is not null
        ) then 0.72
        when x0.offer_3_3_variants = 'Free Fountain Drink + 2x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6035', '6085')
            or z2.loyaltymemberkey is not null
        ) then 0.22
        when x0.offer_3_3_variants = '250 bonus points' then 0
        when x0.offer_3_3_variants = '300 Bonus points on order-ahead or delivery purchase' then 0
        when x0.offer_3_3_variants = 'Get $0 Delivery Fee'
        and z0.offercatalogcampaignkey = 6371 then 1
        when x0.offer_3_3_variants = 'Free Fountain Drink + 3x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6368', '6367')
            or z2.loyaltymemberkey is not null
        ) then 0.22
        when x0.offer_3_3_variants = '2x points on next purchase' then 0
        when x0.offer_3_3_variants = '150 bonus points on next purchase' then 0
        when x0.offer_3_3_variants = 'Free Fountain Drink'
        and z0.offercatalogcampaignkey in ('7026') then 0.22
        when x0.offer_3_3_variants = 'Free Side and Chips'
        and z0.offercatalogcampaignkey in ('7027') then 1.20
        when x0.offer_3_3_variants = 'Free Fountain Drink with Chips'
        and z0.offercatalogcampaignkey in ('7032') then 0.22
        when x0.offer_3_3_variants = '350 bonus points on click'
        and z2.loyaltymemberkey is not null then 0
        when x0.offer_3_3_variants = '3x points on next purchase'
        and z2.loyaltymemberkey is not null then 0
    end as redemption_hard_cost_offer2,
    case
        when x0.offer_3_3_variants = 'Choice of Side with Chips + 2x points multiplier on next purchase'
        and (
            z0.offercatalogcampaignkey = 6034
            or z2.loyaltymemberkey is not null
        ) then coalesce(
            z2.total_points_awarded * 0.0044,
            _2nd_txn_netsales * 10 * 0.0044
        )
        when x0.offer_3_3_variants = 'Free Fountain Drink + 2x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6035', '6085')
            or z2.loyaltymemberkey is not null
        ) then coalesce(
            z2.total_points_awarded * 0.0044,
            _2nd_txn_netsales * 10 * 0.0044
        )
        when x0.offer_3_3_variants = '250 bonus points'
        and z2.loyaltymemberkey is not null then (z2.total_points_awarded * 0.0044)
        when x0.offer_3_3_variants = '300 Bonus points on order-ahead or delivery purchase'
        and z2.loyaltymemberkey is not null then (z2.total_points_awarded * 0.0044)
        when x0.offer_3_3_variants = 'Get $0 Delivery Fee' then 0
        when x0.offer_3_3_variants = 'Free Fountain Drink + 3x points multiplier on the order'
        and (
            z0.offercatalogcampaignkey in ('6368', '6367')
            or z2.loyaltymemberkey is not null
        ) then coalesce(
            z2.total_points_awarded * 0.0044,
            _2nd_txn_netsales * 20 * 0.0044
        )
        when x0.offer_3_3_variants = '2x points on next purchase' then (z2.total_points_awarded * 0.0044)
        when x0.offer_3_3_variants = '150 bonus points on next purchase' then (z2.total_points_awarded * 0.0044)
        when x0.offer_3_3_variants = 'Free Fountain Drink'
        and z0.offercatalogcampaignkey in ('7026') then 0
        when x0.offer_3_3_variants = 'Free Side and Chips'
        and z0.offercatalogcampaignkey in ('7027') then 0
        when x0.offer_3_3_variants = 'Free Fountain Drink with Chips'
        and z0.offercatalogcampaignkey in ('7032') then 0
        when x0.offer_3_3_variants = '350 bonus points on click'
        and z2.loyaltymemberkey is not null then (z2.total_points_awarded * 0.0044)
        when x0.offer_3_3_variants = '3x points on next purchase'
        and z2.loyaltymemberkey is not null then (z2.total_points_awarded * 0.0044)
    end as redemption_points_cost_offer2,
    y0.offer_4_3_variants,
    case
        when offer_4_3_variants = '150 Bonus points on next purchase' then 'Offer3A'
        when offer_4_3_variants = '2X points on next purchase' then 'Offer3B'
        when offer_4_3_variants = '150 Bonus points on order-ahead or delivery purchase' then 'Offer3C'
        when offer_4_3_variants = '200 Bonus points on next purchase' then 'Non Digital Offer3B'
        when offer_4_3_variants = '3X points on digital purchase' then 'Offer3D3'
        when offer_4_3_variants = 'Free Side' then 'Offer3D'
    end as offer_4_3_variants_short,
    case
        when z3.name is not null then z3.name
        when y0.offer_4_3_variants = 'Free Side'
        and z01.loyaltymemberkey is not null then 'Offer 3D: Free Side'
    end as offer_4_3_variant_redeemed,
    case
        when y0.offer_4_3_variants = '150 Bonus points on next purchase' then 0
        when y0.offer_4_3_variants = '2X points on next purchase' then 0
        when y0.offer_4_3_variants = '150 Bonus points on order-ahead or delivery purchase' then 0
        when y0.offer_4_3_variants = '200 Bonus points on next purchase' then 0
        when y0.offer_4_3_variants = '3X points on digital purchase' then 0
        when y0.offer_4_3_variants = 'Free Side'
        and z01.offercatalogcampaignkey in ('6369', '6463') then 0.70
        else null
    end as redemption_hard_cost_offer3,
    case
        when y0.offer_4_3_variants = '150 Bonus points on next purchase'
        and z3.loyaltymemberkey is not null then 1
        when y0.offer_4_3_variants = '2X points on next purchase'
        and z3.loyaltymemberkey is not null then 1
        when y0.offer_4_3_variants = '150 Bonus points on order-ahead or delivery purchase'
        and z3.loyaltymemberkey is not null then 1
        when y0.offer_4_3_variants = '200 Bonus points on next purchase'
        and z3.loyaltymemberkey is not null then 1
        when y0.offer_4_3_variants = '3X points on digital purchase'
        and z3.loyaltymemberkey is not null then 1
        when y0.offer_4_3_variants = 'Free Side'
        and z01.loyaltymemberkey is not null then 1
    end as has_redeemed_offer3,
    case
        when y0.offer_4_3_variants = '150 Bonus points on next purchase'
        and z3.loyaltymemberkey is not null then z3.redemptiondate
        when y0.offer_4_3_variants = '2X points on next purchase'
        and z3.loyaltymemberkey is not null then z3.redemptiondate
        when y0.offer_4_3_variants = '150 Bonus points on order-ahead or delivery purchase'
        and z3.loyaltymemberkey is not null then z3.redemptiondate
        when y0.offer_4_3_variants = '200 Bonus points on next purchase'
        and z3.loyaltymemberkey is not null then z3.redemptiondate
        when y0.offer_4_3_variants = '3X points on digital purchase'
        and z3.loyaltymemberkey is not null then z3.redemptiondate
        when y0.offer_4_3_variants = 'Free Side'
        and z01.loyaltymemberkey is not null then z01.redemptiondate
    end as redemption_date_offer3,
    case
        when y0.offer_4_3_variants = '150 Bonus points on next purchase'
        and z3.loyaltymemberkey is not null then (z3.total_points_awarded * 0.0044)
        when y0.offer_4_3_variants = '2X points on next purchase'
        and z3.loyaltymemberkey is not null then (z3.total_points_awarded * 0.0044)
        when y0.offer_4_3_variants = '150 Bonus points on order-ahead or delivery purchase'
        and z3.loyaltymemberkey is not null then (z3.total_points_awarded * 0.0044)
        when y0.offer_4_3_variants = '200 Bonus points on next purchase'
        and z3.loyaltymemberkey is not null then (z3.total_points_awarded * 0.0044)
        when y0.offer_4_3_variants = '3X points on digital purchase'
        and z3.loyaltymemberkey is not null then (z3.total_points_awarded * 0.0044)
        when y0.offer_4_3_variants = 'Free Side' then 0
    end as redemption_point_cost_offer3,
    1 as is_in_1st_txn_journey,
    case
        when _1st_txn_date is not null then 1
        else 0
    end as is_in_2nd_txn_journey,
    case
        when _2nd_txn_date is not null then 1
        else 0
    end as is_in_3rd_txn_journey,
    a2.total_mails_received,
    a2.total_mails_received_1st_purchase,
    a2.total_mails_received_2nd_purchase,
    a2.total_mails_received_3rd_purchase,
    kx0.senttime as tch_1_1_tstamp,
    ky0.senttime as tch_1_2_tstamp,
    k0.senttime as tch_2_1_tstamp,
    l0.senttime as tch_2_2_tstamp,
    l01.senttime as tch_2_6_tstamp,
    m0.senttime as tch_2_3_tstamp,
    n0.senttime as tch_2_4_tstamp,
    p0.senttime as tch_2_5_tstamp,
    q0.senttime as tch_3_1_tstamp,
    r0.senttime as tch_3_2_tstamp,
    s0.senttime as tch_3_4_tstamp,
    t0.senttime as tch_3_5_tstamp,
    u0.senttime as tch_4_1_tstamp,
    v0.senttime as tch_4_2_tstamp,
    w0.senttime as tch_4_4_tstamp,
    x0.senttime as tch_3_3_tstamp,
    y0.senttime as tch_4_3_tstamp,
    ntj_2_8.senttime as tch_2_8_tstamp,
    kx0.bounce_timestamp as tch_1_1_bounce_tstamp,
    ky0.bounce_timestamp as tch_1_2_bounce_tstamp,
    k0.bounce_timestamp as tch_2_1_bounce_tstamp,
    l0.bounce_timestamp as tch_2_2_bounce_tstamp,
    l01.bounce_timestamp as tch_2_6_bounce_tstamp,
    m0.bounce_timestamp as tch_2_3_bounce_tstamp,
    n0.bounce_timestamp as tch_2_4_bounce_tstamp,
    p0.bounce_timestamp as tch_2_5_bounce_tstamp,
    q0.bounce_timestamp as tch_3_1_bounce_tstamp,
    r0.bounce_timestamp as tch_3_2_bounce_tstamp,
    s0.bounce_timestamp as tch_3_4_bounce_tstamp,
    t0.bounce_timestamp as tch_3_5_bounce_tstamp,
    u0.bounce_timestamp as tch_4_1_bounce_tstamp,
    v0.bounce_timestamp as tch_4_2_bounce_tstamp,
    w0.bounce_timestamp as tch_4_4_bounce_tstamp,
    x0.bounce_timestamp as tch_3_3_bounce_tstamp,
    y0.bounce_timestamp as tch_4_3_bounce_tstamp,
    ntj_2_8.bounce_timestamp as tch_2_8_bounce_tstamp,
    kx0.first_open_timestamp as tch_1_1_1st_open_tstamp,
    ky0.first_open_timestamp as tch_1_2_1st_open_tstamp,
    k0.first_open_timestamp as tch_2_1_1st_open_tstamp,
    l0.first_open_timestamp as tch_2_2_1st_open_tstamp,
    l01.first_open_timestamp as tch_2_6_1st_open_tstamp,
    m0.first_open_timestamp as tch_2_3_1st_open_tstamp,
    n0.first_open_timestamp as tch_2_4_1st_open_tstamp,
    p0.first_open_timestamp as tch_2_5_1st_open_tstamp,
    q0.first_open_timestamp as tch_3_1_1st_open_tstamp,
    r0.first_open_timestamp as tch_3_2_1st_open_tstamp,
    s0.first_open_timestamp as tch_3_4_1st_open_tstamp,
    t0.first_open_timestamp as tch_3_5_1st_open_tstamp,
    u0.first_open_timestamp as tch_4_1_1st_open_tstamp,
    v0.first_open_timestamp as tch_4_2_1st_open_tstamp,
    w0.first_open_timestamp as tch_4_4_1st_open_tstamp,
    x0.first_open_timestamp as tch_3_3_1st_open_tstamp,
    y0.first_open_timestamp as tch_4_3_1st_open_tstamp,
    ntj_2_8.first_open_timestamp as tch_2_8_open_tstamp,
    kx0.first_click_timestamp as tch_1_1_1st_click_tstamp,
    ky0.first_click_timestamp as tch_1_2_1st_click_tstamp,
    k0.first_click_timestamp as tch_2_1_1st_click_tstamp,
    l0.first_click_timestamp as tch_2_2_1st_click_tstamp,
    l01.first_click_timestamp as tch_2_6_1st_click_tstamp,
    m0.first_click_timestamp as tch_2_3_1st_click_tstamp,
    n0.first_click_timestamp as tch_2_4_1st_click_tstamp,
    p0.first_click_timestamp as tch_2_5_1st_click_tstamp,
    q0.first_click_timestamp as tch_3_1_1st_click_tstamp,
    r0.first_click_timestamp as tch_3_2_1st_click_tstamp,
    s0.first_click_timestamp as tch_3_4_1st_click_tstamp,
    t0.first_click_timestamp as tch_3_5_1st_click_tstamp,
    u0.first_click_timestamp as tch_4_1_1st_click_tstamp,
    v0.first_click_timestamp as tch_4_2_1st_click_tstamp,
    w0.first_click_timestamp as tch_4_4_1st_click_tstamp,
    x0.first_click_timestamp as tch_3_3_1st_click_tstamp,
    y0.first_click_timestamp as tch_4_3_1st_click_tstamp,
    ntj_2_8.first_click_timestamp as tch_2_8_1st_click_tstamp,
    kx0.unsubscribed_timestamp as tch_1_1_unsub_tstamp,
    ky0.unsubscribed_timestamp as tch_1_2_unsub_tstamp,
    k0.unsubscribed_timestamp as tch_2_1_unsub_tstamp,
    l0.unsubscribed_timestamp as tch_2_2_unsub_tstamp,
    l01.unsubscribed_timestamp as tch_2_6_unsub_tstamp,
    m0.unsubscribed_timestamp as tch_2_3_unsub_tstamp,
    n0.unsubscribed_timestamp as tch_2_4_unsub_tstamp,
    p0.unsubscribed_timestamp as tch_2_5_unsub_tstamp,
    q0.unsubscribed_timestamp as tch_3_1_unsub_tstamp,
    r0.unsubscribed_timestamp as tch_3_2_unsub_tstamp,
    s0.unsubscribed_timestamp as tch_3_4_unsub_tstamp,
    t0.unsubscribed_timestamp as tch_3_5_unsub_tstamp,
    u0.unsubscribed_timestamp as tch_4_1_unsub_tstamp,
    v0.unsubscribed_timestamp as tch_4_2_unsub_tstamp,
    w0.unsubscribed_timestamp as tch_4_4_unsub_tstamp,
    x0.unsubscribed_timestamp as tch_3_3_unsub_tstamp,
    y0.unsubscribed_timestamp as tch_4_3_unsub_tstamp,
    ntj_2_8.unsubscribed_timestamp as tch_2_8_unsub_tstamp,
    case
        when _1st_txn_date is not null
        and tch_1_2_tstamp is null
        and tch_2_3_tstamp is null then 'Without nudge'
        when _1st_txn_date < tch_1_2_tstamp then 'Without nudge'
        when _1st_txn_date is not null
        and tch_1_2_tstamp is null
        and tch_2_3_tstamp is not null
        and _1st_txn_date <= tch_2_3_tstamp then 'Without nudge'
        when _1st_txn_date is not null
        and tch_1_2_tstamp is not null
        and tch_2_3_tstamp is null
        and _1st_txn_date > tch_1_2_tstamp then 'Touch 1.2'
        when _1st_txn_date is not null
        and tch_1_2_tstamp is not null
        and tch_2_3_tstamp is not null
        and _1st_txn_date > tch_1_2_tstamp
        and _1st_txn_date <= tch_2_3_tstamp then 'Touch 1.2'
        when _1st_txn_date is not null
        and tch_2_3_tstamp is not null
        and tch_2_4_tstamp is null then 'Touch 2.3'
        when _1st_txn_date is not null
        and tch_2_3_tstamp is not null
        and tch_2_4_tstamp is not null
        and tch_2_3_tstamp < _1st_txn_date
        and _1st_txn_date <= tch_2_4_tstamp then 'Touch 2.3'
        when _1st_txn_date is not null
        and tch_2_4_tstamp is not null
        and tch_2_5_tstamp is null then 'Touch 2.4'
        when _1st_txn_date is not null
        and tch_2_4_tstamp is not null
        and tch_2_5_tstamp is not null
        and tch_2_4_tstamp < _1st_txn_date
        and _1st_txn_date <= tch_2_5_tstamp then 'Touch 2.4'
        when _1st_txn_date is not null
        and tch_2_5_tstamp is not null
        and _1st_txn_date > tch_2_5_tstamp then 'Touch 2.5'
        when _1st_txn_date is not null
        and tch_2_8_tstamp is not null
        and _1st_txn_date > tch_2_8_tstamp then 'Touch 2.8'
    end as _1st_txn_attribution,
    case
        when _2nd_txn_date is not null
        and (
            tch_2_2_tstamp is not null
            or tch_2_1_tstamp is not null
        )
        and (
            _2nd_txn_date < tch_2_2_tstamp
            or _2nd_txn_date < tch_1_2_tstamp
        ) then 'Without nudge'
        when _2nd_txn_date is not null
        and tch_2_2_tstamp is null
        and tch_2_1_tstamp is null then 'Without nudge'
        when _2nd_txn_date is not null
        and (
            tch_2_2_tstamp is not null
            or tch_2_1_tstamp is not null
        )
        and tch_3_2_tstamp is null then 'Touch 2.1/2.2'
        when _2nd_txn_date is not null
        and (
            tch_2_2_tstamp is not null
            or tch_2_1_tstamp is not null
        )
        and _2nd_txn_date <= tch_3_2_tstamp then 'Touch 2.1/2.2'
        when _2nd_txn_date is not null
        and tch_3_2_tstamp is not null
        and tch_3_3_tstamp is null then 'Touch 3.2'
        when _2nd_txn_date is not null
        and tch_3_2_tstamp is not null
        and tch_3_3_tstamp is not null
        and tch_3_2_tstamp < _2nd_txn_date
        and _2nd_txn_date <= tch_3_3_tstamp then 'Touch 3.2'
        when _2nd_txn_date is not null
        and tch_3_3_tstamp is not null
        and tch_3_4_tstamp is null then 'Touch 3.3'
        when _2nd_txn_date is not null
        and tch_3_3_tstamp is not null
        and tch_3_4_tstamp is not null
        and tch_3_3_tstamp < _2nd_txn_date
        and _2nd_txn_date <= tch_3_4_tstamp then 'Touch 3.3'
        when _2nd_txn_date is not null
        and tch_3_4_tstamp is not null
        and tch_3_5_tstamp is null then 'Touch 3.4'
        when _2nd_txn_date is not null
        and tch_3_4_tstamp is not null
        and tch_3_5_tstamp is not null
        and tch_3_4_tstamp < _2nd_txn_date
        and _2nd_txn_date <= tch_3_5_tstamp then 'Touch 3.4'
        when _2nd_txn_date is not null
        and tch_3_5_tstamp is not null
        and datediff(day, cast(tch_3_5_tstamp as date), _2nd_txn_date) <= 1 then 'Touch 3.5'
        when _2nd_txn_date is not null
        and tch_3_5_tstamp is not null
        and datediff(day, cast(tch_3_5_tstamp as date), _2nd_txn_date) > 1 then 'Post offer 2 Expiry'
    end as _2nd_txn_attribution,
    case
        when _3rd_txn_date is not null
        and _3rd_txn_date < tch_3_1_tstamp then 'Without nudge'
        when _3rd_txn_date is not null
        and tch_3_1_tstamp is null
        and tch_4_2_tstamp is null then 'Without nudge'
        when _3rd_txn_date is not null
        and tch_3_1_tstamp is not null
        and tch_4_2_tstamp is null
        and _3rd_txn_date > tch_3_1_tstamp then 'Touch 3.1'
        when _3rd_txn_date is not null
        and tch_4_2_tstamp is not null
        and tch_4_3_tstamp is null then 'Touch 4.2'
        when _3rd_txn_date is not null
        and tch_4_2_tstamp is not null
        and tch_4_3_tstamp is not null
        and tch_4_2_tstamp < _3rd_txn_date
        and _3rd_txn_date <= tch_4_3_tstamp then 'Touch 4.2'
        when _3rd_txn_date is not null
        and tch_4_3_tstamp is not null
        and tch_4_4_tstamp is null then 'Touch 4.3'
        when _3rd_txn_date is not null
        and tch_4_3_tstamp is not null
        and tch_4_4_tstamp is not null
        and tch_4_3_tstamp < _3rd_txn_date
        and _3rd_txn_date <= tch_4_4_tstamp then 'Touch 4.3'
        when _3rd_txn_date is not null
        and tch_4_4_tstamp is not null
        and datediff(day, cast(tch_4_4_tstamp as date), _3rd_txn_date) <= 1 then 'Touch 4.4'
        when _3rd_txn_date is not null
        and tch_4_4_tstamp is not null
        and datediff(day, cast(tch_4_4_tstamp as date), _3rd_txn_date) > 1 then 'Post offer 3 Expiry'
    end as _3rd_txn_attribution,
    /*case when j0._3rd_txn_date is not null or datediff(day, a0.created_date, current_date) > 90 
    	then 1 
    	else 0 
    	end as has_exited_journey,
	case when j0._3rd_txn_date is not null 
		then j0._3rd_txn_date 
		else dateadd(day, 90, a0.created_date) 
		end as date_exited_journey*/
	case when a0.ntj_entry_date is null -- didn't enter NTJ
        	then case when j0._3rd_txn_date is not null or datediff(day, a0.created_date, current_date) > 90
		        then 1 
		        else 0
            end 
           when a0.ntj_entry_date is not null -- Entered NTJ
           then NULL
	end  as has_exited_journey,
	case when a0.ntj_entry_date is null -- didn't enter NTJ
        then case when j0._3rd_txn_date is not null 
		        then j0._3rd_txn_date 
		        else dateadd(day, 90, a0.created_date) 
            end 
        when a0.ntj_entry_date is not null -- Entered NTJ
        then NULL
	end as date_exited_journey
from (
        select distinct loyaltymemberkey,
            customerkey,
            loyalty_member_id,
            external_id,
            contact_id as subscriberkey,
            subscriberid,
            accountcreationdatetime,
            signup_date,
            cust_tag,
            first_touch,
            journey_join_date,
            created_date,
            journey_type,
            ntj_entry_date,
            ntj_entry_flag,
            wj_re_entry_date,
            wj_re_entry_flag
        from {TEMP_SCHEMA}.welcome_journey_loyaltymemberkey_contactid_mapping
        where loyaltymemberkey is not null
            and created_date >= '2020-11-14'
            --and ntj_entry_flag=0
            --or wj_re_entry_date < ntj_entry_date
    ) a0 
    --new section
    left join 
    (select distinct external_id
        from (
                select distinct external_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup_bkup
                where lower(controlgroup) in ('wj_cg')
                    and event_date is not null
                union all
                select distinct external_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where lower(controlgroup) in ('wj_cg')
                    and control_event_date is not null
            )
    ) b0 
    ON a0.external_id = b0.external_id
    left join 
    (select distinct external_id
        from (
                select external_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup_bkup
                where lower(controlgroup) in ('wj_offer_2_holdout')
                    and event_date is not null
                union all
                select external_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where lower(controlgroup) in ('wj_offer_2_holdout')
                    and control_event_date is not null
                union all
                select external_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where touch3_holdout_type is not null
            )
    ) c0 
    ON a0.external_id = c0.external_id
    left join 
    (select distinct external_id
        from (
                select external_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup_bkup
                where lower(controlgroup) in ('wj_offer_3_holdout')
                    and event_date is not null
                union all
                select external_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where lower(controlgroup) in ('wj_offer_3_holdout')
                    and control_event_date is not null
                union all
                select external_id
                from {TARGET_SCHEMA}.sfmc_Welcome_Journeys_ControlGroup
                where touch4_holdout_type is not null
            )
    ) d0 
    ON a0.external_id = d0.external_id --end here
    left join 
    (select loyaltymemberkey,
            signup_date,
            _1st_txn_date,
            _1st_txn_purchase_channel,
            _1st_txn_netsales,
            _1st_txn_saleschannelname,
            _2nd_txn_date,
            _2nd_txn_purchase_channel,
            _2nd_txn_netsales,
            _2nd_txn_saleschannelname,
            _3rd_txn_date,
            _3rd_txn_purchase_channel,
            _3rd_txn_netsales,
            _3rd_txn_saleschannelname,
            -- case when _1st_txn_purchase_channel in ('WHITELABEL', 'ORDER AHEAD') then 1 else 0 end as is_1st_txn_digital,
            datediff(day, _1st_txn_date, _2nd_txn_date) as days1_2_purchase,
            datediff(day, _2nd_txn_date, _3rd_txn_date) as days2_3_purchase
        from (
                select loyaltymemberkey,
                    checkkey,
                    checktime as _1st_txn_date,
                    signup_date,
                    created_date,
                    purchase_channel as _1st_txn_purchase_channel,
                    purchase_channel as _1st_txn_saleschannelname,
                    netsalesamount as _1st_txn_netsales,
                    loyalty_txn_number,
                    lead(checktime, 1) over(partition by loyaltymemberkey order by loyalty_txn_number) as _2nd_txn_date,
                    lead(purchase_channel, 1) over(partition by loyaltymemberkey order by loyalty_txn_number) as _2nd_txn_purchase_channel,
                    lead(netsalesamount, 1) over(partition by loyaltymemberkey order by loyalty_txn_number) as _2nd_txn_netsales,
                    lead(purchase_channel, 1) over(partition by loyaltymemberkey order by loyalty_txn_number) as _2nd_txn_saleschannelname,
                    lead(checktime, 2) over(partition by loyaltymemberkey order by loyalty_txn_number) as _3rd_txn_date,
                    lead(purchase_channel, 2) over( partition by loyaltymemberkey order by loyalty_txn_number) as _3rd_txn_purchase_channel,
                    lead(netsalesamount, 2) over(partition by loyaltymemberkey order by loyalty_txn_number) as _3rd_txn_netsales,
                    lead(purchase_channel, 2) over(partition by loyaltymemberkey order by loyalty_txn_number) as _3rd_txn_saleschannelname
                from (
                        select x1.loyaltymemberkey,
                            x1.checkkey,
                            x1.purchase_channel,
                            x1.checkdate,
                            x1.checktime,
                            x1.netsalesamount,
                            y1.signup_date,
                            y1.created_date,
                            y1.ntj_entry_flag,
                            row_number() over(partition by x1.loyaltymemberkey order by checkdate,checkkey) as loyalty_txn_number
                        from 
                        (select loyaltymemberkey,
                                    checkkey,
                                    purchase_channel,
                                    checkdate,
                                    checktime,
                                    netsalesamount
                                from {TARGET_SCHEMA}.cdp_loyalty_all_transaction_master
                                where txn_number_post_signup is not null
                                and purchase_channel not in ('MARKETPLACE')
                        ) x1
                        inner join 
                        (select distinct loyaltymemberkey,
                                    customerkey,
                                    accountcreationdatetime,
                                    signup_date,
                                    ntj_entry_flag,
                                    created_date
                                from {TEMP_SCHEMA}.welcome_journey_loyaltymemberkey_contactid_mapping
                                where loyaltymemberkey is not null
                                    and created_date >= '2020-11-14'
                                    and ntj_entry_flag=0 
                                    or wj_re_entry_date < ntj_entry_date
                         ) y1 
                        ON x1.loyaltymemberkey = y1.loyaltymemberkey
                        where (checkdate between dateadd(day, -1, created_date) and dateadd(day, 90, created_date))
                    )
            )
        where loyalty_txn_number = 1
    ) j0 
    ON a0.loyaltymemberkey = j0.loyaltymemberkey
    left join 
    (select x1.loyaltymemberkey,
            sum(x1.netsalesamount) as total_Sales_90_days,
            count(checkkey) as total_txns_90_days
        from (
                select loyaltymemberkey,
                    checkkey,
                    checkdate,
                    netsalesamount
                from {TARGET_SCHEMA}.cdp_loyalty_all_transaction_master
                where txn_number_post_signup is not null
            ) x1
            inner join (
                select distinct loyaltymemberkey,
                    created_date,
                    ntj_entry_flag
                from {TEMP_SCHEMA}.welcome_journey_loyaltymemberkey_contactid_mapping
                where loyaltymemberkey is not null
                and ntj_entry_flag=0 
                or wj_re_entry_date < ntj_entry_date
            ) y1 
            ON x1.loyaltymemberkey = y1.loyaltymemberkey
        where (checkdate between dateadd(day, -1, created_date)
                and dateadd(day, 90, created_date))
        group by x1.loyaltymemberkey
    ) j1 
    ON a0.loyaltymemberkey = j1.loyaltymemberkey
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where upper(emailname) like 'WJ%1.1%BRAND%'
    ) kx0 ON a0.subscriberid = kx0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where upper(emailname) LIKE 'WJ%1.2%BRAND%'
    ) ky0 ON a0.subscriberid = ky0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname LIKE 'WJ%2.1%'
    ) k0 ON a0.subscriberid = k0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname LIKE 'WJ%2.2%'
    ) l0 ON a0.subscriberid = l0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname LIKE 'WJ%2.6%'
    ) l01 ON a0.subscriberid = l01.subscriberid
      left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname LIKE 'WJ%2.3%'
    ) m0 ON a0.subscriberid = m0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname LIKE 'WJ%2.4%'
    ) n0 ON a0.subscriberid = n0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname LIKE 'WJ%2.5_Nudge'
    ) p0 ON a0.subscriberid = p0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname LIKE 'WJ%3.1_Post_Purchase'
    ) q0 ON a0.subscriberid = q0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname LIKE 'WJ_3.2_Brand_Nudge%'
    ) r0 ON a0.subscriberid = r0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname like 'WJ_%3.4%'
    ) s0 ON a0.subscriberid = s0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname like 'WJ_%3.5_%'
    ) t0 ON a0.subscriberid = t0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname like 'WJ%4.1_Post_Purchase'
    ) u0 ON a0.subscriberid = u0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname LIKE 'WJ%4.2%'
    ) v0 ON a0.subscriberid = v0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where emailname like 'WJ_%4.4%'
    ) w0 ON a0.subscriberid = w0.subscriberid
    -- change
	left join (
        select subscriberkey,
            subscriberid,
            sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp
        from {TEMP_SCHEMA}.wj_campaign_engagement_master
        where ((upper(emailname) like ('%WJ%') and emailname like ('%2.8%'))
		or (upper(emailname) like ('%NTJ%') and emailname like ('%2.8%'))emailname like 'NTJ_%2.8%')
    ) ntj_2_8 ON a0.subscriberid = ntj_2_8.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            a1.sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp,
            case
                        when emailname = 'WJ_3.3_Brand_Offer_Offer2A' then 'Choice of Side with Chips + 2x points multiplier on next purchase'
                        when emailname = 'WJ_3.3_Brand_Offer_Offer2B' then 'Free Fountain Drink + 2x points multiplier on the order'
                        when emailname = 'WJ_3.3_Brand_Offer_Offer2C' then '250 bonus points'
                        when emailname = 'WJ_3.3_Brand_Offer_Offer2D' then '300 Bonus points on order-ahead or delivery purchase'
                        when emailname LIKE 'WJ_3.3_Brand_Digital_Offer_2B%'
                        or emailname LIKE 'WJ_V3_3.3 Brand Offer Digital 2B%' then 'Free Fountain Drink + 2x points multiplier on the order'
                        when emailname LIKE 'WJ_3.3_Brand_Digital_Offer_2C2%'
                        or emailname LIKE 'WJ_V3_3.3 Brand Offer Digital 2C2%' then 'Get $0 Delivery Fee' 
                        when emailname LIKE 'WJ_3.3_Brand_Digital_Offer_2D2%'
                        or emailname LIKE 'WJ_V3_3.3_Brand_Offer_Digital_2D2' then 'Free Fountain Drink + 3x points multiplier on the order'
                        when emailname LIKE 'WJ_3.3_Brand_Offer_2A' then 'Choice of Side with Chips + 2x points multiplier on next purchase'
                        when emailname LIKE 'WJ_3.3_Brand_Offer_2D' then '2x points on next purchase'
                        when emailname LIKE 'WJ_3.3_Brand_Offer_2E'
                        or emailname LIKE 'WJ_V3_3.3_Non_Digital_Nudge_offer_2E' then '150 bonus points on next purchase'
                        when emailname LIKE 'WJ%3.3%Brand_Offer_Non_Digital_Offer_2A1' then 'Free Fountain Drink'
                        when emailname LIKE 'WJ%3.3%Brand_Offer_Non_Digital_Offer_2A2' then 'Free Side and Chips'
                        when emailname LIKE 'WJ%3.3%Brand_Offer_Non_Digital_Offer_2C' then 'Free Fountain Drink with Chips'
                        when emailname LIKE 'WJ%3.3%Brand_Offer_Non_Digital_Offer_2F' then '350 bonus points on click'
                        when emailname LIKE 'WJ%3.3%Brand_Offer_Digital_Offer_2D' then '3x points on next purchase'
                    end as offer_3_3_variants
        from {TEMP_SCHEMA}.wj_campaign_engagement_master a1
        where emailname like '%WJ%3.3_%'
         and senttime > '2020-11-01'
    ) x0 ON a0.subscriberid = x0.subscriberid
    left join (
        select subscriberkey,
            subscriberid,
            a1.sendid,
            senttime,
            first_open_timestamp,
            first_click_timestamp,
            has_unsubscribed,
            unsubscribed_timestamp,
            bounce_timestamp,
            case
                        when emailname = 'WJ_4.3_Offer_Offer3A'
                        or emailname LIKE 'WJ_V3_4.3_Offer_3A' then '150 Bonus points on next purchase'
                        when emailname = 'WJ_4.3_Offer_Offer3B' then '2X points on next purchase'
                        when emailname = 'WJ_4.3_Offer_Offer3C' then '150 Bonus points on order-ahead or delivery purchase'
                        when emailname LIKE 'WJ_4.3_Digital_Offer_3C_SL%' then '150 Bonus points on order-ahead or delivery purchase'
                        when emailname LIKE 'WJ_4.3_Digital_Offer_3D_SL%' then 'Free Side'
                        when emailname LIKE 'WJ_4.3_Nudge_Offer_3D'
                        or emailname LIKE 'WJ_V3_4.3_Offer_3D' then 'Free Side'
                        when emailname LIKE 'WJ_4.3_Nudge_Offer_3A' then '150 Bonus points on next purchase'
                        when emailname LIKE 'WJ%4.3%Offer_3B' then '200 Bonus points on next purchase'
                        when emailname LIKE 'WJ%4.3%Offer_3D3' then '3X points on digital purchase'
                    end as offer_4_3_variants
        from {TEMP_SCHEMA}.wj_campaign_engagement_master a1
        where emailname like 'WJ%4.3_%'
        and senttime > '2020-11-01'
    ) y0 ON a0.subscriberid = y0.subscriberid
    left join (
        select *
        from {TEMP_SCHEMA}.wj_hard_rewards_redemption_master
        where offercatalogcampaignkey in (
                '6035',
                '6034',
                '6085',
                '6371',
                '6368',
                '6367',
                '7026',
                '7027',
                '7032'
            )
            and loyaltymemberkey is not null
    ) z0 ON a0.loyaltymemberkey = z0.loyaltymemberkey
    left join (
        select *
        from {TEMP_SCHEMA}.wj_hard_rewards_redemption_master
        where offercatalogcampaignkey in ('6369', '6463')
            and loyaltymemberkey is not null
    ) z01 ON a0.loyaltymemberkey = z01.loyaltymemberkey
    left join (
        select *
        from {TEMP_SCHEMA}.wj_hard_rewards_redemption_master
        where offercatalogcampaignkey in ('6036', '2371', '6964', '7120') -- (5th Change)
            and loyaltymemberkey is not null
    ) z1 ON a0.loyaltymemberkey = z1.loyaltymemberkey
    left join (
        select *
        from {TEMP_SCHEMA}.wj_soft_rewards_redemption_master
        where name in (
                'SFMC Welcome Journey TEST',
                'Welcome Journey Offer 2-A',
                'Welcome Journey Offer 2-B',
                'Welcome Journey Offer 2C',
                'SFMC Welcome Journey Offer 2D',
                'Welcome Journey 2D-2 3x Points',
                'Welcome Journey 2D-2 3x Points - Replatformed',
                'SFMC Welcome Journey Offer 2 b TEST',
                'SFMC Welcome Journey Offer 2',
                'SFMC Welcome Journey Offer 2A',
                'SFMC Welcome Journey Offer 2B',
                'SFMC Welcome Journey Offer 2C',
                'SFMC Welcome Journey Offer 2-D',
                'Welcome Journey 2-D reconfig',
                'SFMC Welcome Journey Offer 2 - Replatformed',
                'SFMC Welcome Journey Offer 2A - Replatformed',
                'SFMC Welcome Journey Offer 2B - Replatformed',
                'SFMC Welcome Journey Offer 2C - Replatformed',
                'SFMC Welcome Journey Offer 2-D - Replatformed',
                'Welcome, 2D Non-Digital, 2x Points',
                'Welcome, 2E Non-Digital, 150 Points',
                'SFMC Welcome Journey Phase 2+3 Events',
                'Offer2A',
                'Offer2B',
                'Offer2C',
                'Offer2D_300',
                'Offer_2F',
                'Offer2D_2',
                'Offer_2D',
                'Offer_2E',
                'Offer_2D_3x'
            )
    ) z2 on a0.loyaltymemberkey = z2.loyaltymemberkey
    left join (
        select *
        from {TEMP_SCHEMA}.wj_soft_rewards_redemption_master
        where name in (
                'Welcome Journey Offer 3A',
                'Welcome Journey Offer 3B',
                'Welcome Journey Offer 3C',
                'SFMC Welcome Journey Offer 3',
                'SFMC Welcome Journey Offer 3A',
                'SFMC Welcome Journey Offer 3B',
                'SFMC Welcome Journey Offer 3C',
                'SFMC Welcome Journey Offer 3 - Replatformed',
                'Welcome Journey Offer 3A - Replatformed',
                'Welcome Journey Offer 3B - Replatformed',
                'Welcome Journey Offer 3C - Replatformed',
                'SFMC Welcome Journey Phase 2+3 Events',
                'Offer3A',
                'Offer3B',
                'Offer3C',
                'Offer_3B',
                'Offer3D3'
            )
    ) z3 on a0.loyaltymemberkey = z3.loyaltymemberkey
    left join 
    (select loyaltymemberkey,
            count(distinct checkkey) as txns_before_wj
        from (
                select x2.loyaltymemberkey,
                    checkkey,
                    checkdate,
                    created_date
                from (
                        select loyaltymemberkey,
                            checkkey,
                            checkdate
                        from {TARGET_SCHEMA}.cdp_loyalty_all_transaction_master
                        where txn_number_post_signup is not null 
                    ) x2
                    inner join (
                        select *
                        from {TEMP_SCHEMA}.welcome_journey_loyaltymemberkey_contactid_mapping
                        where created_date >= '2020-11-14'
                    ) y2 ON x2.loyaltymemberkey = y2.loyaltymemberkey
            )
        where checkdate <= dateadd(day, -2, created_date) 
        group by loyaltymemberkey
    ) z4 
    ON a0.loyaltymemberkey = z4.loyaltymemberkey
    left join (
        select subscriberkey,
            subscriberid,
            loyaltymemberkey,
            count(distinct sendid) as total_mails_received,
            count(
                distinct case
                    when emailname like 'WJ%1.2%'
                    or emailname like 'WJ%2.3%'
                    or emailname like 'WJ%2.4%'
                    or emailname like 'WJ%2.6%' 
                    or ((upper(emailname) like '%WJ%' and emailname like '%2.8%')
					or (upper(emailname) like '%NTJ%' and emailname like '%2.8%'))
                    then sendid
                end
            ) as total_mails_received_1st_purchase,
            --updated          
            count(
                distinct case
                    when emailname like 'WJ%2.1%'
                    or emailname like 'WJ%2.2%'
                    or emailname like 'WJ%3.2%'
                    or emailname like '%WJ%3.3%'
                    or emailname like 'WJ%3.5%'
                    or emailname like 'WJ%3.4%'
                    or emailname like 'WJ%2.6%' then sendid
                end
            ) as total_mails_received_2nd_purchase,
            count(
                distinct case
                    when emailname like 'WJ%3.1%'
                    or emailname like 'WJ%4.2%'
                    or emailname like 'WJ%4.1%'
                    or emailname like 'WJ%4.3%'
                    or emailname like 'WJ%4.4%' then sendid
                end
            ) as total_mails_received_3rd_purchase
        from {TEMP_SCHEMA}.wj_campaign_engagement_master 
        group by subscriberkey,
            subscriberid,
            loyaltymemberkey
    ) a2 on a0.subscriberid = a2.subscriberid;

drop table if exists {TABLEAU_SCHEMA}.welcome_journey_non_ntj_master;
create table {TABLEAU_SCHEMA}.welcome_journey_non_ntj_master as
    select * from {TEMP_SCHEMA}.welcome_journey_non_ntj_master;

grant select on {TABLEAU_SCHEMA}.welcome_journey_non_ntj_master to public ;
