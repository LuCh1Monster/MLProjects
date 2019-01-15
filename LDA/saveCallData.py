from impala.dbapi import connect
from impala.util import as_pandas
from config import impala_settings
import pyprind

conn = connect(**impala_settings)
cur = conn.cursor()

def getOwneridLst():

    sql = r"""select distinct(owner_id) 
    from cszc.hsy_asr_pretreatment_3
    where substr(file_date, 1, 7) = '2018-05'
    """
    cur.execute(sql)
    owners = cur.fetchall()
    return owners

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
