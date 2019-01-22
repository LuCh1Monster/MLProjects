from impala.dbapi import connect
from impala.util import as_pandas
from config import impala_settings
import pyprind

conn = connect(**impala_settings)
cur = conn.cursor()

def getOwneridLst():

    sql = r"""select distinct(owner_id) 
    from cszc.hsy_asr_pretreatment_4
    where substr(file_date, 1, 7) = '2018-05'
    """
    cur.execute(sql)
    owners = cur.fetchall()
    return owners


def saveCallTextDataWithAgentsIncludeAll():

    owners = getOwneridLst()
    pbar = pyprind.ProgBar(len(owners))

    for ownerid in owners:

        sql = r"""select *  from (
        select tb2.owner_id
        ,tb1.file_date
        ,tb1.callid
        ,tb1.channel
        ,group_concat(tb1.content) as 'content'
        from cszc.hsy_asr_pretreatment_2 tb1
        left join [shuffle]
        cszc.hsy_asr_pretreatment_3 tb2
        on tb1.callid = tb2.callid
        where tb1.channel = 'AGENT'
        group by tb2.owner_id, tb1.file_date, tb1.callid, tb1.channel
        order by tb2.owner_id, tb1.file_date, tb1.callid, tb1.channel) tb
        where tb.owner_id = %s
        and substr(tb.file_date, 1, 7) = '2018-05'
        """

        cur.execute(sql, (ownerid[0],))
        df = as_pandas(cur)
        outputfile = './data/%s.csv' % ownerid[0]
        df.to_csv(outputfile, index=False, encoding='utf8')
        pbar.update()

def saveCallTextDataWithAgentsExceptSelf():
    """包含绩效经办绩效排名"""
    owners = getOwneridLst()
    pbar = pyprind.ProgBar(len(owners))

    for ownerid in owners:

        sql = r"""select tb2.owner_id
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
            and tb2.owner_id = %s
            """

        cur.execute(sql, (ownerid[0],))
        df = as_pandas(cur)
        outputfile = './dataExceptSelf/%s.csv' % ownerid[0]
        df.to_csv(outputfile, index=False, encoding='utf8')
        pbar.update()

saveCallTextDataWithAgentsExceptSelf()
cur.close()
conn.close()
