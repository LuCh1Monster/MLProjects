-- Date: 2019/01/22 By: yumingmin --
-- 通话数据重构维度表:
-- [基本维度]
-- 1. 经办ID ：owner_id
-- 2. 文件日期 : file_date
-- 3. 通话ID : callid
-- 4. 当前文本的说话角色 : channel
-- 5. 通话文本 : content
-- [添加维度]
-- 1. 当前通话是本人还是联系人 : phone_type
-- 2.


select tb2.owner_id
,tb3.jx_rank
,tb1.file_date
,tb1.callid
,tb1.channel
,tb1.content
,tb2.duration
,tb2.phone_type
,tb2.time_mark
,tb2.group_name
,tb2.is_first_yes_call
,tb2.yes_call_num
,tb2.day_num
from (
    select file_date
    ,callid
    ,channel
    ,group_concat(content) as 'content'
    from cszc.hsy_asr_pretreatment_2
    where substr(file_date, 1, 7) = '2018-05'
    group by file_date, callid, channel) tb1
left join [shuffle] cszc.hsy_asr_pretreatment_4 tb2
on tb1.callid = tb2.callid
left join [shuffle] (select owner_id
    ,tot_case_num
    ,repay_rate
    ,concat(cast(cast(repay_rate*100 as decimal(38,2)) as string),'%') as repay_rate_2
    ,row_number() over(order by repay_rate desc) as jx_rank
    from
        (select owner_id
        ,count(distinct dun_case_id) as tot_case_num
        ,sum(dun_repay_amount)/sum(start_owing_amount) as repay_rate
        from cszc.mrjx_dp_201805
        where model like '%M2大额%'
        group by 1) tt) tb3
on tb2.owner_id = tb3.owner_id
where tb1.channel = 'AGENT'

-- 绩效前20和后20的经办ID
select owner_id, jx_rank_desc from
    (select owner_id
        ,tot_case_num
        ,repay_rate
        ,concat(cast(cast(repay_rate*100 as decimal(38,2)) as string),'%') as repay_rate_2
        ,row_number() over(order by repay_rate desc) as jx_rank_desc
        ,row_number() over(order by repay_rate asc) as jx_rank_asc
        from
            (select owner_id
            ,count(distinct dun_case_id) as tot_case_num
            ,sum(dun_repay_amount)/sum(start_owing_amount) as repay_rate
            from cszc.mrjx_dp_201805
            where model like '%M2大额%'
            group by 1) tt
    ) tb
where jx_rank_desc <= 10
or jx_rank_asc <= 10
order by jx_rank_desc

