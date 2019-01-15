---------------------*调整seq_id新的顺序，合并分开的语句*---------------------
---------------------*运行至11月的数据*---------------------
drop table if exists cszc.hsy_asr_pretreatment_2;
create table if not exists cszc.hsy_asr_pretreatment_2 as 
select b.file_date
,a.callid
,a.channel
,a.content,
row_number() over (partition by a.callid order by (cast(split_part(a.new_name,'_',3) as int))) as seq_id,
b.speed
from 
(
select new_name
,file_name as callid
,rt_role as channel
,group_concat(rt_text) as content
from cszc.hsy_asr_pretreatment_1
group by 1,2,3
) a
left join [shuffle]
(
select file_name
,rt_role,file_date
,sum(length(rt_text)*rt_speed)/sum(length(rt_text)) as speed
from ods.rt_trans_result
group by 1,2,3
) b 
on a.callid=b.file_name and a.channel=b.rt_role;
compute stats cszc.hsy_asr_pretreatment_2;


--补充信息phone\owner_id\user_id\dun_case_id\contact_type\phone_type
---------------------*11月*---------------------
drop table if exists cszc.hsy_phone_type_Nov_M2;
create table if not exists cszc.hsy_phone_type_Nov_M2;

---------------------*话务逻辑修改*---------------------
--4、5、6月使用edw.fact_collection_reach_jnss_daily
--7月之后使用ods.sip_record
--增加变量，是否首次接通、是否首次拨打（其实就是对拨打次数、接通次数排个序，但要确保是这次分给经办的案子，且一直在经办手下）
--首次分案的时间，ods.tb_case_allocation，其中time_mark为首次分到该经办名下的时间
--Aspect公共池部分，每次都要重新分案，所以每次都会有个time_mark；月初要重新分案，又有一次新的time_mark；一个用户或者一个dun_case_id会有多个对应的time_mark
--以下的新案都是指这个案子在分案之后第一天分给经办
--月初1号都是新案
--2-31号如下
--user_id，找对应的分案时间，每通电话都要根据电话时间找出最近那次分案时间，而且在这个分案时间上逾期天数在31-60天之内
--dun_case_id，只要找对应owner_id下的分案时间。
---------------------*code*---------------------

--先找每个callid对应的time_mark，user_id，dun_case_id

--5、6月份的
--dun_case_id从分案表里找
--time_mark也从分案表里找
drop table if exists cszc.hsy_asr_pretreatment_3;
create table if not exists cszc.hsy_asr_pretreatment_3 as 
select * from 
(
select callid,file_date,phone,ringtime,duration,user_id,owner_id,owner_name,group_name,dun_case_id,to_date(time_mark) as time_mark
from 
( 
select *,
       row_number() over(partition by callid,user_id order by time_mark desc) as rn 
from 
(
select distinct a.callid,a.file_date,b.dnis as phone,b.ringtime ,b.t_durtion as duration,b.user_id,b.owner_id,b.owner_name,b.group_name,
       c.time_mark,c.dun_case_id	   
from cszc.hsy_asr_pretreatment_2 a 
left join [shuffle] edw.fact_collection_reach_jnss_daily b on a.callid=b.calluuid
left join [shuffle] 
                   (select * from ods.tb_case_allocation 
				    where overdue_max_days>=31 and overdue_max_days<=60 
					      and to_date(time_mark)>='2018-04-01' and to_date(time_mark)<'2018-07-01') c 
					on b.user_id=c.borrow_user_id and to_date(c.time_mark)<=to_date(b.ringtime)
where a.file_date<='2018-06-31' and b.user_id is not null and c.time_mark is not null
) temp1
) temp2 
where rn=1	

--从edw.fact_collection_reach_jnss_daily出发，有些user_id为空值，号码不为空，但不知道这个号码是属于谁的，本人还是联系人，无法确认user_id，则剔除，5、6月共8242通
--31条没有匹配到time_mark，为0531、0620、0630那三天的，那几件案件的逾期天数在分案表里是显示在M1模块中，而在话务表中显示在M2模块中。
--基于上述情况，剔除这31条

--7月份以后
--话务表ods.sip_record,没有一条callid存在两条记录
--owner_id需要从ods.tb_group_user中获取
union 
select callid,file_date,phone,ringtime,duration,user_id,owner_id,owner_name,group_name,dun_case_id,to_date(time_mark) as time_mark
from 
( 
select *,
       row_number() over(partition by callid,user_id order by time_mark desc) as rn 
from 
(
select distinct a1.callid,a1.file_date,
       b1.dnis as phone,b1.ringtime ,b1.t_durtion as duration,b1.user_id,b1.owner_id,b1.owner_name,b1.group_name,b1.dun_case_id,
	   c.time_mark
from cszc.hsy_asr_pretreatment_2 a1 

left join [shuffle]
(
select distinct a.calluuid,to_date(a.createtime) dt,a.ringtime,c.group_name,c.owner_id,c.owner_name,
cast (a.ud_1 as int) as user_id,cast (a.ud_2 as int) as dun_case_id,
a.dnis,a.t_durtion
from 
(
select * from ods.sip_record
where createtime >= '2018-07-01'
      and t_durtion>0
)a
left join ods.tb_group_user b
on a.dn = b.extensionnumber
inner join 
(select distinct owner_id,owner_name,group_name,dt 
from edw.fact_ppd_collection_user_group_relation 
where dt >= '2018-07-01' and dt <to_date(now())  and 
group_name like  '%M2大额%' and group_name is not NULL ) c
on b.user_id = c.owner_id and to_date(a.createtime) = c.dt
) b1 on a1.callid=b1.calluuid


left join [shuffle] 
                   (select * from ods.tb_case_allocation 
				    where overdue_max_days>=31 and overdue_max_days<=60 
					      and to_date(time_mark)>='2018-07-01' and to_date(time_mark)<'2019-01-01') c 
					on b1.user_id=c.borrow_user_id and to_date(c.time_mark)<=to_date(b1.ringtime)
					
where a1.file_date>'2018-06-31' and b1.dun_case_id is not null and time_mark is not null
) temp1 
) temp2 
where rn=1
) temp;
compute stats cszc.hsy_asr_pretreatment_3;	
--从ods.sip_record出发，有些user_id、dun_case_id为空值，则剔除，7-11月共289通
--156条没有匹配到time_mark，剔除待验证???

'''
--检查edw.fact_collection_reach_jnss_daily一个calluuid有两条及其以上的记录。
select callid,count(*)
from 
(
select distinct a.callid,a.file_date,b.dnis as phone,b.ringtime ,b.t_durtion as duration,b.user_id,b.owner_id,b.group_name
from cszc.hsy_asr_pretreatment_2 a 
left join [shuffle] edw.fact_collection_reach_jnss_daily b on a.callid=b.calluuid
where a.file_date<='2018-06-31' and b.user_id is not null
) temp
group by callid having count(*)>1
--共479个callid存在一个电话有两个user_id的情况。
--后面想根据user_id实际逾期情况判断。
'''

---------------------*话务其他信息补充*---------------------
--按照晨哥他们的逻辑，对每一个号码进行归类。
--根据字段time_mark,user_id,dun_case_id按照ring_time排序，获取这个案子这次在这个经办名下是第几天接通，以及第几次接通
--五月份因为只有11日开始的数据，所以数据不全，需要修正第几次接通，根据time_mark小于ringtime排序
--如果只看新案的话，五月符合两个条件,is_first_yes_call='新案' and yes_call_num=1;6月之后只要yes_call_num=1
---------------------*code*---------------------
--号码关系层级，可参考ddm.fact_user_cs_phone_recommend
--链接字段user_id,phone
--逻辑参考晨哥
'''
case when order_id in (1,2) then 'self'
                      when order_id in (3,4,5) then 'close_contact'
                      when order_id in (6,7) then 'top_contact'
                      else 'others'
                 end as order_name

'''

--ddm.fact_user_cs_phone_recommend会存在一样的记录,49235个phone,user_id,dt
drop table if exists cszc.hsy_asr_pretreatment_4;
create table if not exists cszc.hsy_asr_pretreatment_4 as 
select a.*,
       case when b.order_id in (1,2) then 'self'
            when b.order_id in (3,4,5) then 'close_contact'
			when b.order_id in (6,7) then 'top_contact'
       else 'others'
       end as phone_type,
	   case when a.time_mark=to_date(a.ringtime) then '新案' else '老案' end as is_first_yes_call,
	   row_number() over (partition by a.user_id,a.time_mark order by a.ringtime) as yes_call_num,
       datediff(a.ringtime,a.time_mark) as day_num 
from cszc.hsy_asr_pretreatment_3 a 
left join [shuffle]
     (
	  select user_id,phone,dt,min(order_id) as order_id
	  from ddm.fact_user_cs_phone_recommend 
	  group by 1,2,3
	 ) b 
on a.user_id=b.user_id and a.phone=b.phone and to_date(a.ringtime)=b.dt;
compute stats cszc.hsy_asr_pretreatment_4;	   




drop table if exists cszc.hsy_jingbanxingwei_M2;
create table cszc.hsy_jingbanxingwei_M2 as
--统计每个seq_id
--经办
select temp1.file_date as dt,temp1.owner_id,temp.*,
temp1.phone_type as contact_type,
--A类
--mention_repayment_time
case when (content REGEXP '今天|明天|后天|早上|早晨|上午|中午|下午|晚上|今晚|点钟?之?前|小时|天之内|天内')=true then 'A' else 'false' end as mention_repayment_time,
--mention_owing_amount
case when (content REGEXP '(拖|欠款|欠|逾期|借|贷款|待还|到期|还款|还)[^几]{1,10}(块|元|千|百)')=true then 'A' else 'false' end as mention_owing_amount,
--mention_default_days
case when (content REGEXP '(拖|欠|逾期|借|贷款|到期)[^今明后]{0,5}(天|月)')=true then 'A' else 'false' end as mention_default_days,
--B类
--persuasion_money_loss
case when 
cast(channel='AGENT' as int)*(0.1*cast(content REGEXP '^.*每.*' as int)+
0.1*cast(content REGEXP '^.*拖.*' as int)+
0.1*cast(content REGEXP '^.*六点七.*' as int)+
0.2*cast(content REGEXP '^.*(增长|涨|越高).*' as int)+
0.2*cast(content REGEXP '^.*产生.*' as int)+
0.2*cast(content REGEXP '^.*承担.*' as int)+
0.2*cast(content REGEXP '^.*不必要|额外.*' as int)+
0.3*cast(content REGEXP '^.*(划不来|不划算).*' as int))*cast(content REGEXP '^.*(费用|罚息|罚金|利息|罚款|滞纳金|违约金|六点七).*' as int)>0.3 
then 'B' else 'false' 
end as  persuasion_money_loss,
--persuasion_credit_loss
case when 
cast(channel='AGENT' as int)*(0.3*cast(content REGEXP '^.*(联|连)网.*' as int)+
0.3*cast(content REGEXP '^.*记录.*' as int)+
0.2*cast(content REGEXP '^.*上传.*' as int)+
0.2*cast(content REGEXP '^.*互联网.*' as int)+
0.1*cast(content REGEXP '^.*维护.*' as int)+
0.1*cast(content REGEXP '^.*个人.*' as int)+
0.1*cast(content REGEXP '^.*(累积|累计).*' as int)+
0.3*cast(content REGEXP '^.*买(车|房).*' as int)+
0.3*cast(content REGEXP '^.*上征信.*' as int)+
0.3*cast(content REGEXP '^.*拉黑.*' as int)+
0.1*cast(content REGEXP '^.*影响.*' as int)+
0.2*cast(content REGEXP '^.*人民银行|央行|芝麻.*' as int)+
0.1*cast(content REGEXP '^.*消除.*' as int)+
0.1*cast(content REGEXP '^.*名誉.*' as int))*cast(content REGEXP '^.*(上?征信|信誉|上?黑名单|(信用[^社卡借贷])).*' as int)>0.4 
then 'B' else 'false' 
end as persuasion_credit_loss,
--persuasion_contact
case when 
cast(channel='AGENT' as int)*(0.3*cast(content REGEXP '^.*笑话|嘲笑.*' as int)+
0.3*cast(content REGEXP '^.*搞得.*' as int)+
0.1*cast(content REGEXP '^.*影响.*' as int)+
0.1*cast(content REGEXP '^.*联系.*' as int)+
0.2*cast(content REGEXP '^.*名声|声誉.*' as int)+
0.3*cast(content REGEXP '^.*眼光.*' as int)+
0.3*cast(content REGEXP '^.*面子.*' as int))*cast(content REGEXP '^.*(家人朋友|亲友|亲戚朋友|亲朋好友|居委|村委|邻居|街坊).*' as int)>0.4 
then 'B' else 'false' 
end as persuasion_contact,
--persuasion_law
case when 
cast(channel='AGENT' as int)*(0.3*cast(content REGEXP '^.*调查.*' as int)+
0.3*cast(content REGEXP '^.*户籍.*' as int)+
0.3*cast(content REGEXP '^.*签收.*' as int)+
0.3*cast(content REGEXP '^.*违约.*' as int)+
0.2*cast(content REGEXP '^.*流程|移交.*' as int)+
0.3*cast(content REGEXP '^.*核实.*' as int)+
0.3*cast(content REGEXP '^.*走.*' as int)+
0.3*cast(content REGEXP '^.*身份证.*' as int))*cast(content REGEXP '^.*(寄送?|派出所|备案|涉嫌|法律|案件|程序|信函).*' as int)>0 
then 'B' else 'false' 
end as persuasion_law,
--persuasion_crime
case when (content REGEXP '^.*(诉讼|事务所|司法|刑法|诈骗|合同法|违法|犯罪|骗贷|公安局|报案|法院|传票|开庭|受审|辩护).*')=true then 'B' else 'false' end as persuasion_crime,
--C类
--check_last_phone
case when 
cast(channel='AGENT' as int)*(cast(content REGEXP '(，|。|！|？)?[^，。！？]*(上次|之前|上一次|以前|上一通|上通)(，|。|！|？)?[^，。！？]*电话[^，。！？]*(，|。|！|？)' as int)+
cast(content REGEXP '(，|。|！|？)?[^，。！？]*(上次|之前|上一次|以前|上一通|上通)(，|。|！|？)?[^，。！？]*(您|你)(，|。|！|？)?[^，。！？]*(说|答应|讲)[^，。！？]*(，|。|！|？)' as int)+
cast(content REGEXP '(?:给|和|跟)(?:你|您)(?:打过电话|来过电话)' as int)+	
cast(content REGEXP '(，|。|！|？)?[^，。！？]*(上次|之前|上一次|以前|上一通|上通)(，|。|！|？)?[^，。！？]*(说|答应|讲)(，|。|！|？)?[^，。！？]*(我们|我)?[^，。！？]*(，|。|！|？)' as int)+
cast(content REGEXP '有没有(?:给|和|跟)(?:你|您)(?:打电话|打过电话|来过电话)' as int)+	
cast(content REGEXP '(?:您|你)上次说' as int)+	
cast(content REGEXP '(?:上次|之前|上一次|以前|有)问(?:您|你)的时候' as int)+	
cast(content REGEXP '(?:您|你)跟我们工作人员' as int)+	
cast(content REGEXP '(?:上次|之前|上一次|以前|有)(?:给|和|跟)(?:您|你)说' as int)
)>0 
then 'C' else 'false' 
end as check_last_phone,
--D类
--is_refusal_time_bargain
tempd1.is_refusal_time_bargain,
--F类
--is_argue
case when 
cast(channel='AGENT' as int)*(cast(content REGEXP '(，|。|！|？)?[^，。！？]*你[^，。！？]*(答应)[^，。！？]*(，|。|！|？)' as int)+
cast(content REGEXP '(，|。|！|？)?[^，。！？]*你[^，。！？]*(不守信用)[^，。！？]*(，|。|！|？)' as int)+
cast(content REGEXP '(，|。|！|？)?[^，。！？]*说好[^，。！？多吧呀吗呢(不好)]+(，|。|！|？)' as int)+
cast(content REGEXP '(，|。|！|？)?[^，。！？]*说好[^，。！？多吧呀吗呢(不好)]+(，|。|！|？)' as int))>0 
then 'F' else 'false' 
end as is_argue,
--G类
--is_debtor_nearby
case when temp1.phone_type='self' then null
     when cast(channel='AGENT' as int)*(cast(content REGEXP '(，|。|！|？)?[^，。！？]*在[^，。！？]*(旁边|身边|你这里)[^，。！？]*(，|。|！|？)' as int)+
          cast(content REGEXP '(，|。|！|？)?[^，。！？]*在不在[^，。！？]*(，|。|！|？)' as int))>0 
          then 'G' else 'false' 
     end as is_debtor_nearby,
--H类
--is_knowing
case when temp1.phone_type='self' then null
     when (content REGEXP '(，|。|！|？)?[^，。！？]*(认识|联系(到|上)|有联系|跟(您|你)联系)[^，。！？]*(，|。|！|？)')=true then 'H' else 'false' end as is_knowing,
--I类
--is_askRelation
case when temp1.phone_type='self' then null
     when (content REGEXP '(，|。|！|？)?[^，。！？]*你[^，。！？]*(朋友|亲戚|同事|学生|老师|爸|妈)[^，。！？]*(，|。|！|？)')=true then 'I' else 'false' end as is_askRelation,
--reaction_notKnowing
tempd2.reaction_notKnowing_anfu,
tempd2.reaction_notKnowing_fangqi,
tempd2.reaction_notKnowing_zhiwen,
tempd2.reaction_notKnowing_qingqiu,
--check_other_call
case when cast(content REGEXP '(，|。|！|？)?[^，。！？]*我[^，。！？]*(打|联系|通话)[^，。！？]*(过|了)[^来]{0,}[^，。！？]*(，|。|！|？)' as int)>0 then 'true' else 'false' end as check_other_call,
--mention_address
case when cast(content REGEXP '(，|。|！|？)?[^，。！？]*(市|省|自治区)[^，。！？]*(县|区|乡|湾|镇)[^，。！？]*(，|。|！|？)' as int)>0 then 'true' else 'false' end as mention_address,
--mention_number
case when cast(content REGEXP '(，|。|！|？)?[^，。！？]*号码[^，。！？]*(零|幺|二|四|五|六|七|八|九)[^，。！？]*(，|。|！|？)' as int)>0 then 'true' else 'false' end as mention_number,
--is_provpmorigin
case when cast(content REGEXP '^.*(他|她|本人)留(到|在|的|你|了|下).*' as int)+cast (content REGEXP '^.*((第三|投资)(人|方))留(到|在|的|你|了|下).*' as int)>0 then 'true' else 'false' end as is_provpmorigin,
--mention_photo
case when cast(content REGEXP '照片' as int)*cast(content REGEXP '(身份证|个人|背景|资料|你|他|借款|贷款|拍)' as int)>0 then 'true' else 'false' end as mention_photo
from cszc.hsy_asr_pretreatment_2 temp
left join [shuffle]
(
--D类
--is_refusal_time_bargain
select b.callid,b.seq_id,
 case when cast(b.content REGEXP '不' as int)+cast(b.content REGEXP '否' as int)>0 then 'D' else 'false' end as is_refusal_time_bargain
from 
(
select callid,seq_id
from cszc.hsy_asr_pretreatment_2
where channel='AGENT' and 
(cast(content REGEXP '(?:今天|明天|后天)上午' as int)+
cast(content REGEXP '(?:今天|明天|后天)下午' as int)+
cast(content REGEXP '(?:今|明|后|几|这几|过几|等几|缓几|这两|一|二|三|四|五|六|七|八|九|十|十几)天' as int)+
cast(content REGEXP '(?:一|二|三|四|五|六|七|八|九|十|十一|十二|晚|晚一|一两|两三|三四|四五|五六|六七|七八|九十)点' as int)+
cast(content REGEXP '(?:一|二|三|四|五|六|七|八|九|十|十一|十二|一两|两三|三四|四五|五六|六七|七八|九十)点钟' as int)+
cast(content REGEXP '下午(?:一|二|三|四|五|六|七|八|九|十|十一|十二|晚|晚一|一两|两三|三四|四五|五六|六七|七八|九十)点' as int)+
cast(content REGEXP '下午(?:一|二|三|四|五|六|七|八|九|十|十一|十二|一两|两三|三四|四五|五六|六七|七八|九十)点钟' as int)+
cast(content REGEXP '上午(?:一|二|三|四|五|六|七|八|九|十|十一|十二|晚|晚一|一两|两三|三四|四五|五六|六七|七八|九十)点' as int)+
cast(content REGEXP '上午(?:一|二|三|四|五|六|七|八|九|十|十一|十二|一两|两三|三四|四五|五六|六七|七八|九十)点钟' as int)+
cast(content REGEXP '(?:一|二|两|三|四|五|六|七|八|九|十|十一|十二|十三|十四|十五|十六|十七|十八|十九|二十|二十一|二十二|二十三|二十四|二十五|二十六|二十七|二十八|二十九|三十|三十一)号' as int)+
cast(content REGEXP '(?:一|二|三|四|五|六|七|八|九|十)个小时还|(?:一|二|三|四|五|六|七|八|九|十)个半小时还|半小时还|半个小时' as int)+
cast(content REGEXP '周(?:一|二|三|四|五|六|末)' as int)+
cast(content REGEXP '(?:这|过几|一|二|三|四|五|六|七|八|九|十|几|下|下下)周' as int)+
cast(content REGEXP '这(?:一|二|三|四|五|六|七|八|九|十|几)周' as int)+
cast(content REGEXP '(?:这|过几|一|二|三|四|五|六|七|八|九|十)个月' as int)+
cast(content REGEXP '这(?:一|二|三|四|五|六|七|八|九|十|几|下|下下)个月' as int)+
cast(content REGEXP '星期(?:一|二|三|四|五|六|日)|下午|中午|晚上|上午|尽量|等一下|等下' as int)+
cast(content REGEXP '(?:下班|等下班|发工资)还' as int))>0
) a 
inner join 
(select * from cszc.hsy_asr_pretreatment_2 ) b on a.callid=b.callid and a.seq_id=b.seq_id-1
) tempd1 on temp.callid=tempd1.callid and temp.seq_id=tempd1.seq_id 
left join [shuffle]
(
--reaction_notKnowing
select b.callid,b.seq_id,
      case when cast(channel='AGENT' as int)*cast(content REGEXP '(，|。|！|？)?[^，。！？]*(别(担心|误会)|放心|只是|不是说)[^，。！？]*(，|。|！|？)' as int)>0 then 'J' else 'false' end as reaction_notKnowing_anfu,
	  case when cast(channel='AGENT' as int)*cast(content REGEXP '(，|。|！|？)?[^，。！？]*(再见)[^，。！？]*(，|。|！|？)' as int)>0 then 'M' else 'false' end as reaction_notKnowing_fangqi,
	  case when cast(channel='AGENT' as int)*cast(content REGEXP '(，|。|！|？)?[^，。！？]*(您不?是|号码|手机号|不要跟我说|怎么可能|确定|频繁)[^，。！？]*(，|。|！|？)' as int)>0 then 'K' else 'false' end as reaction_notKnowing_zhiwen,
	  case when cast(channel='AGENT' as int)*cast(content REGEXP '(，|。|！|？)?[^，。！？]*(看一下|号码|紧急联系人|手机号|打个电话|如果能|转告)[^，。！？]*(，|。|！|？)' as int)>0 then 'L' else 'false' end as reaction_notKnowing_qingqiu
from 
(
--客户提不认识的场景
select callid,seq_id
from cszc.hsy_asr_pretreatment_2
where channel='USER' and 
     (cast(content REGEXP '(，|。|！|？)?[^，。！？]*(跟|与|和|给)(，|。|！|？)?[^，。！？]*(无|没)[a-zA-Z0-9_\u4e00-\u9fa5]*(关系|关)[^，。！？]*(，|。|！|？)' as int)+
      cast(content REGEXP '不认识' as int)+
      cast(content REGEXP '打错了' as int)+
      cast(content REGEXP '毫无关系' as int))>0
) a
inner join 
(select * from cszc.hsy_asr_pretreatment_2 ) b on a.callid=b.callid and a.seq_id=b.seq_id-1
) tempd2 on temp.callid=tempd2.callid and temp.seq_id=tempd2.seq_id 
left join [shuffle] cszc.hsy_asr_pretreatment_4 temp1
on temp.callid=temp1.callid
where channel='AGENT';
compute stats cszc.hsy_jingbanxingwei_M2;


--用户
drop table if exists cszc.hsy_yonghuxingwei_M2;
create table cszc.hsy_yonghuxingwei_M2 as
select temp.*,
--2
--is_askingmethod
case when (content REGEXP '(，|。|！|？)?[^，。！？]*((不会|怎么样?)还|还不进去?)[^，。！？]*(，|。|！|？)')=true then '2' else 'false' end as is_askingMethod,
--7
--is_rejection
case when 
(cast(content REGEXP '(，|。|！|？)?[^，。！？]*(不|为什么|怎么)(，|。|！|？)?[^，。！？]*转告[^，。！？]*(，|。|！|？)' as int)+			
cast(content REGEXP '就不还|就是不还|不想还钱|没钱还|确实还不了|还不了|就是故意不还|确定不还|没钱' as int))>0 
then '7' else 'false' end as is_rejection,
--9
--mention_nocall
case when 
(cast(content REGEXP '(，|。|！|？)?[^，。！？]*(不要)(，|。|！|？)?[^，。！？]*(联系|电话)[^，。！？]*(，|。|！|？)' as int)+
cast(content REGEXP '骚扰' as int))>0 
then '9' else 'false' end as mention_noCall,
--is_complaint
case when (channel='USER' and content REGEXP '(，|。|！|？)?[^，。！？]*我[^，。！？]*投诉[^，。！？]*(，|。|！|？)')=true then 'true' else 'false' end as is_complaint,
--4
--is_notknowing
case when contact_type='self' then null
     when (content REGEXP '(，|。|！|？)?[^，。！？]*(不认识|没联系)[^，。！？]*(，|。|！|？)')=true then '4' else 'false' end as is_notKnowing,
--6
--mention_noconnection
case when contact_type='self' then null 
     when (cast(content REGEXP '(，|。|！|？)?[^，。！？]*(跟|与|和|给)(，|。|！|？)?[^，。！？]*(无|没)[a-zA-Z0-9_\u4e00-\u9fa5]*(关系|关)[^，。！？]*(，|。|！|？)' as int)+
           cast(content REGEXP '不认识' as int)+
           cast(content REGEXP '打错了' as int)+
           cast(content REGEXP '毫无关系' as int))>0 
     then '6' else 'false' end as mention_noConnection,
--1
--is_askingreduction
case when contact_type!='self' then null 
     when (content REGEXP '(，|。|！|？)?[^，。！？]*(减免|减少一?点)[^，。！？]*(，|。|！|？)')=true then '1' else 'false' end as is_askingReduction,
--3 
--time_bargain
case when contact_type!='self' then null 
     when cast(channel='USER' as int)*(cast(content REGEXP '(?:今天|明天|后天)上午' as int)+
          cast(content REGEXP '(?:今天|明天|后天)下午' as int)+
          cast(content REGEXP '(?:今|明|后|几|这几|过几|等几|缓几|这两|一|二|三|四|五|六|七|八|九|十|十几)天' as int)+
		  cast(content REGEXP '(?:一|二|三|四|五|六|七|八|九|十|十一|十二|晚|晚一|一两|两三|三四|四五|五六|六七|七八|九十)点' as int)+
		  cast(content REGEXP '(?:一|二|三|四|五|六|七|八|九|十|十一|十二|一两|两三|三四|四五|五六|六七|七八|九十)点钟' as int)+
		  cast(content REGEXP '下午(?:一|二|三|四|五|六|七|八|九|十|十一|十二|晚|晚一|一两|两三|三四|四五|五六|六七|七八|九十)点' as int)+
		  cast(content REGEXP '下午(?:一|二|三|四|五|六|七|八|九|十|十一|十二|一两|两三|三四|四五|五六|六七|七八|九十)点钟' as int)+
		  cast(content REGEXP '上午(?:一|二|三|四|五|六|七|八|九|十|十一|十二|晚|晚一|一两|两三|三四|四五|五六|六七|七八|九十)点' as int)+
		  cast(content REGEXP '上午(?:一|二|三|四|五|六|七|八|九|十|十一|十二|一两|两三|三四|四五|五六|六七|七八|九十)点钟' as int)+
		  cast(content REGEXP '(?:一|二|两|三|四|五|六|七|八|九|十|十一|十二|十三|十四|十五|十六|十七|十八|十九|二十|二十一|二十二|二十三|二十四|二十五|二十六|二十七|二十八|二十九|三十|三十一)号' as int)+
		  cast(content REGEXP '(?:一|二|三|四|五|六|七|八|九|十)个小时还|(?:一|二|三|四|五|六|七|八|九|十)个半小时还|半小时还|半个小时' as int)+
		  cast(content REGEXP '周(?:一|二|三|四|五|六|末)' as int)+
		  cast(content REGEXP '(?:这|过几|一|二|三|四|五|六|七|八|九|十|几|下|下下)周' as int)+
		  cast(content REGEXP '这(?:一|二|三|四|五|六|七|八|九|十|几)周' as int)+
		  cast(content REGEXP '(?:这|过几|一|二|三|四|五|六|七|八|九|十)个月' as int)+
		  cast(content REGEXP '这(?:一|二|三|四|五|六|七|八|九|十|几|下|下下)个月' as int)+
		  cast(content REGEXP '星期(?:一|二|三|四|五|六|日)|下午|中午|晚上|上午|尽量|等一下|等下' as int)+
		  cast(content REGEXP '(?:下班|等下班|发工资)还' as int))>0 
		  then '3' 
	 else 'false' 
	 end as time_bargain,
--is_helping
case when (content REGEXP '(，|。|！|？)?[^，。！？]*(试|一定)[^，。！？]*(，|。|！|？)')=true then 'true' else 'false' end as is_helping,
--is_investigation
case when contact_type='self' then null
     when cast(content REGEXP '为什么' as int)*cast(content REGEXP '(拖|处理|解决|钱)' as int)>0 then 'true' else 'false' end as is_investigation,
--is_detail
case when contact_type='self' then null
     when cast(content REGEXP '(，|。|！|？)?[^，。！？]*(借|欠|逾期|贷|拖|剩)(，|。|！|？)?[^，。！？]*(多少|多久)[^，。！？]*(，|。|！|？)' as int)>0 then 'true' else 'false' end as is_detail
from 
(
select temp2.owner_id,temp2.file_date as dt,temp1.*,temp2.phone_type as contact_type
from cszc.hsy_asr_pretreatment_2 temp1
left join [shuffle] cszc.hsy_asr_pretreatment_4 temp2 on temp1.callid=temp2.callid
) temp
where channel='USER';
compute stats cszc.hsy_yonghuxingwei_M2;


drop table if exists cszc.hsy_wenbenbianliang_M2;
create table cszc.hsy_wenbenbianliang_M2 as
with base1 as 
(
select owner_id,dt,callid,contact_type,
case when sum(is_askingmethod)>0 then 'true' else 'false' end as is_askingmethod,
case when sum(is_rejection)>0 then 'true' else 'false' end as is_rejection,
case when sum(mention_nocall)>0 then 'true' else 'false' end as mention_nocall,
case when sum(is_complaint)>0 then 'true' else 'false' end as is_complaint,
case when contact_type='逾期本人' then null 
     when sum(is_notknowing)>0 then 'true' else 'false' end as is_notknowing,
case when contact_type='逾期本人' then null 
     when sum(mention_noconnection)>0 then 'true' else 'false' end as mention_noconnection,
case when contact_type='联系人' then null 
     when sum(is_askingreduction)>0 then 'true' else 'false' end as is_askingreduction,
case when contact_type='联系人' then null 
     when sum(time_bargain)>0 then 'true' else 'false' end as time_bargain,
--is_helping,is_investigation,is_detail
case when sum(is_helping)>0 then 'true' else 'false' end as is_helping,
case when contact_type='逾期本人' then null 
     when sum(is_investigation)>0 then 'true' else 'false' end as is_investigation,
case when contact_type='逾期本人' then null 
     when sum(is_detail)>0 then 'true' else 'false' end as is_detail
from 
(
select owner_id,dt,callid,channel,seq_id,contact_type,
case when is_askingmethod='false' or is_askingmethod is Null then 0 else 1 end as is_askingmethod,
case when is_rejection='false' or is_rejection is Null then 0 else 1 end as is_rejection,
case when mention_nocall='false' or mention_nocall is Null then 0 else 1 end as mention_nocall,
case when is_complaint='false' or is_complaint is Null then 0 else 1 end as is_complaint,
case when is_notknowing='false' or is_notknowing is Null then 0 else 1 end as is_notknowing,
case when mention_noconnection='false' or mention_noconnection is Null then 0 else 1 end as mention_noconnection,
case when is_askingreduction='false' or is_askingreduction is Null then 0 else 1 end as is_askingreduction,
case when time_bargain='false' or time_bargain is Null then 0 else 1 end as time_bargain,
--is_helping,is_investigation,is_detail
case when is_helping='false' or is_helping is Null then 0 else 1 end as is_helping,
case when is_investigation='false' or is_investigation is Null then 0 else 1 end as is_investigation,
case when is_detail='false' or is_detail is Null then 0 else 1 end as is_detail
from cszc.hsy_yonghuxingwei_M2
) temp
group by 1,2,3,4
),
--经办
base2 as
(
select base1.*,temp2.mention_repayment_time,temp2.mention_owing_amount,temp2.mention_default_days,
temp2.persuasion_money_loss,temp2.persuasion_credit_loss,temp2.persuasion_contact,temp2.persuasion_law,temp2.persuasion_crime,
temp2.check_last_phone,
case when base1.time_bargain='false' or base1.time_bargain is null then null else temp2.is_refusal_time_bargain end as is_refusal_time_bargain,
temp2.is_argue,temp2.is_debtor_nearby,temp2.is_knowing,temp2.is_askrelation,
case when base1.is_notknowing='false' or base1.is_notknowing is null then null else temp2.reaction_notknowing_anfu end as reaction_notknowing_anfu,
case when base1.is_notknowing ='false' or base1.is_notknowing is null then null else temp2.reaction_notknowing_fangqi end as reaction_notknowing_fangqi,
case when base1.is_notknowing ='false' or base1.is_notknowing is null then null else temp2.reaction_notknowing_zhiwen end as reaction_notknowing_zhiwen,
case when base1.is_notknowing ='false' or base1.is_notknowing is null then null else temp2.reaction_notknowing_qingqiu end as reaction_notknowing_qingqiu,
--check_other_call,mention_address,mention_number,is_provpmorigin,mention_photo
temp2.check_other_call,temp2.mention_address,temp2.mention_number,temp2.is_provpmorigin,temp2.mention_photo
from
(
select owner_id,dt,callid,contact_type,
case when sum(mention_repayment_time)>0 then 'true' else 'false' end as mention_repayment_time,
case when sum(mention_owing_amount)>0 then 'true' else 'false' end as mention_owing_amount,
case when sum(mention_default_days)>0 then 'true' else 'false' end as mention_default_days,
case when sum(persuasion_money_loss)>0 then 'true' else 'false' end as persuasion_money_loss,
case when sum(persuasion_credit_loss)>0 then 'true' else 'false' end as persuasion_credit_loss,
case when sum(persuasion_contact)>0 then 'true' else 'false' end as persuasion_contact,
case when sum(persuasion_law)>0 then 'true' else 'false' end as persuasion_law,
case when sum(persuasion_crime)>0 then 'true' else 'false' end as persuasion_crime,
case when sum(check_last_phone)>0 then 'true' else 'false' end as check_last_phone,
case when sum(is_refusal_time_bargain)>0 then 'true' else 'false' end as is_refusal_time_bargain,
case when sum(is_argue)>0 then 'true' else 'false' end as is_argue,
case when contact_type='逾期本人' then null
     when sum(is_debtor_nearby)>0 then 'true' else 'false' end as is_debtor_nearby,
case when contact_type='逾期本人' then null
     when sum(is_knowing)>0 then 'true' else 'false' end as is_knowing,
case when contact_type='逾期本人' then null
     when sum(is_askrelation)>0 then 'true' else 'false' end as is_askrelation,
case when sum(reaction_notknowing_anfu)>0 then 'true' else 'false' end as reaction_notknowing_anfu,
case when sum(reaction_notknowing_fangqi)>0 then 'true' else 'false' end as reaction_notknowing_fangqi,
case when sum(reaction_notknowing_zhiwen)>0 then 'true' else 'false' end as reaction_notknowing_zhiwen,
case when sum(reaction_notknowing_qingqiu)>0 then 'true' else 'false' end as reaction_notknowing_qingqiu,
--check_other_call,mention_address,mention_number,is_provpmorigin,mention_photo
case when sum(check_other_call)>0 then 'true' else 'false' end as check_other_call,
case when sum(mention_address)>0 then 'true' else 'false' end as mention_address,
case when sum(mention_number)>0 then 'true' else 'false' end as mention_number,
case when sum(is_provpmorigin)>0 then 'true' else 'false' end as is_provpmorigin,
case when sum(mention_photo)>0 then 'true' else 'false' end as mention_photo
from 
(
select owner_id,dt,callid,channel,seq_id,contact_type,
case when mention_repayment_time='false' or mention_repayment_time is Null then 0 else 1 end as mention_repayment_time,
case when mention_owing_amount='false' or mention_owing_amount is Null then 0 else 1 end as mention_owing_amount,
case when mention_default_days='false' or mention_default_days is Null then 0 else 1 end as mention_default_days,
case when persuasion_money_loss='false' or persuasion_money_loss is Null then 0 else 1 end as persuasion_money_loss,
case when persuasion_credit_loss='false' or persuasion_credit_loss is Null then 0 else 1 end as persuasion_credit_loss,
case when persuasion_contact='false' or persuasion_contact is Null then 0 else 1 end as persuasion_contact,
case when persuasion_law='false' or persuasion_law is Null then 0 else 1 end as persuasion_law,
case when persuasion_crime='false' or persuasion_crime is Null then 0 else 1 end as persuasion_crime,
case when check_last_phone='false' or check_last_phone is Null then 0 else 1 end as check_last_phone,
case when is_refusal_time_bargain is Null or is_refusal_time_bargain='false' then 0 else 1 end as is_refusal_time_bargain,
case when is_argue='false' or is_argue is Null then 0 else 1 end as is_argue,
case when is_debtor_nearby='false' or is_debtor_nearby is Null then 0 else 1 end as is_debtor_nearby,
case when is_knowing='false' or is_knowing is Null then 0 else 1 end as is_knowing,
case when is_askrelation='false' or is_askrelation is Null then 0 else 1 end as is_askrelation,
case when reaction_notknowing_anfu='false' or reaction_notknowing_anfu is Null then 0 else 1 end as reaction_notknowing_anfu,
case when reaction_notknowing_fangqi='false' or reaction_notknowing_fangqi is Null then 0 else 1 end as reaction_notknowing_fangqi,
case when reaction_notknowing_zhiwen='false' or reaction_notknowing_zhiwen is Null then 0 else 1 end as reaction_notknowing_zhiwen,
case when reaction_notknowing_qingqiu='false' or reaction_notknowing_qingqiu is Null then 0 else 1 end as reaction_notknowing_qingqiu,
--check_other_call,mention_address,mention_number,is_provpmorigin,mention_photo
case when check_other_call='false' or check_other_call is Null then 0 else 1 end as check_other_call,
case when mention_address='false' or mention_address is Null then 0 else 1 end as mention_address,
case when mention_number='false' or mention_number is Null then 0 else 1 end as mention_number,
case when is_provpmorigin='false' or is_provpmorigin is Null then 0 else 1 end as is_provpmorigin,
case when mention_photo='false' or mention_photo is Null then 0 else 1 end as mention_photo
from cszc.hsy_jingbanxingwei_M2
) temp
group by 1,2,3,4
) temp2 
left join [shuffle] base1 on base1.callid=temp2.callid
where base1.callid is not null
)
select owner_id,dt,callid,contact_type,mention_repayment_time,mention_owing_amount,mention_default_days,
persuasion_money_loss,persuasion_credit_loss,persuasion_contact,persuasion_law,persuasion_crime,
check_last_phone,is_refusal_time_bargain,is_argue,is_debtor_nearby,is_knowing,is_askrelation,
reaction_notknowing_anfu,reaction_notknowing_fangqi,reaction_notknowing_zhiwen,reaction_notknowing_qingqiu,
check_other_call,mention_address,mention_number,is_provpmorigin,mention_photo,
is_askingmethod,is_rejection,mention_nocall,is_complaint,is_notknowing,mention_noconnection,is_askingreduction,
time_bargain,is_helping,is_investigation,is_detail
from base2;
compute stats cszc.hsy_wenbenbianliang_M2;

---------------------*通用变量逻辑修改*---------------------
--增加dun_case_id,time_mark,is_first_yes_call,yes_call_num,day_num
--修改contact_type
--保留ringtime,duration,t_interaction,collector_speed,receiver_speed,ratio_collector_char,jingban_mean_sentence_num等
--是否考虑加入逾期天数、逾期金额等变量
---------------------*code*---------------------
drop table if exists cszc.hsy_tongyongbianliang_M2_v2;
create table cszc.hsy_tongyongbianliang_M2_v2 as
select
a.callid,
b.user_id,
b.dun_case_id,
b.owner_id,
b.file_date as dt,
b.time_mark,
b.is_first_yes_call,
b.yes_call_num,
b.day_num,
b.phone_type as contact_type,
b.ringtime,
b.duration,
a.t_interaction,
i.collector_speed,
j.receiver_speed,
k.ratio_collector_char,
l.jingban_mean_sentence_num,
m.jingban_max_sentence_num,
n.yonghu_mean_sentence_num,
o.yonghu_max_sentence_num
from 
(
select callid,max(seq_id) as t_interaction
from cszc.hsy_asr_pretreatment_2
group by 1
) a
left join cszc.hsy_asr_pretreatment_4 b
on a.callid=b.callid
left join [shuffle]
(select callid,max(speed) as collector_speed from cszc.hsy_asr_pretreatment_2 where channel ='AGENT' group by callid) i on a.callid=i.callid
left join [shuffle]
(select callid,max(speed) as receiver_speed from cszc.hsy_asr_pretreatment_2 where channel ='USER' group by callid) j on a.callid=j.callid
left join [shuffle]
(
select tt1.callid,tt1.collector_num/tt2.all_num as ratio_collector_char 
from 
(select callid,sum(floor(CHAR_LENGTH(content)/3)) as collector_num from cszc.hsy_asr_pretreatment_2 where channel='AGENT' group by 1) tt1
left join [shuffle]
(select callid,sum(floor(CHAR_LENGTH(content)/3)) as all_num from cszc.hsy_asr_pretreatment_2 group by 1) tt2 
on tt1.callid=tt2.callid
) k on a.callid=k.callid
left join [shuffle]
(
select callid,avg(floor(CHAR_LENGTH(content)/3)) as jingban_mean_sentence_num from cszc.hsy_asr_pretreatment_2 where channel='AGENT' group by 1
) l on a.callid=l.callid
left join [shuffle]
(
select callid,max(floor(CHAR_LENGTH(content)/3)) as jingban_max_sentence_num from cszc.hsy_asr_pretreatment_2 where channel='AGENT' group by 1
) m on a.callid=m.callid
left join [shuffle]
(
select callid,avg(floor(CHAR_LENGTH(content)/3)) as yonghu_mean_sentence_num from cszc.hsy_asr_pretreatment_2 where channel='USER' group by 1
) n on a.callid=n.callid
left join [shuffle]
(
select callid,max(floor(CHAR_LENGTH(content)/3)) as yonghu_max_sentence_num from cszc.hsy_asr_pretreatment_2 where channel='USER' group by 1
) o on a.callid=o.callid;
compute stats cszc.hsy_tongyongbianliang_M2_v2;


--汇总变量(缺PTP_pred、OPTP_pred、LMS_pred;collector_interrupt_times,customer_interrupt_times,silence_length(s),silence_length(s)_ratio;pattern)
drop table if exists cszc.hsy_bianliang_M2_v2;
create table cszc.hsy_bianliang_M2_v2 as
select
a.dt,a.owner_id,a.user_id,a.dun_case_id,a.callid,a.contact_type,a.ringtime,a.duration,a.t_interaction,a.collector_speed,a.receiver_speed,
a.ratio_collector_char,a.jingban_mean_sentence_num,a.jingban_max_sentence_num,a.yonghu_mean_sentence_num,a.yonghu_max_sentence_num,
a.time_mark,a.is_first_yes_call,a.yes_call_num,a.day_num,
b.mention_repayment_time,b.mention_owing_amount,b.mention_default_days,	
b.persuasion_money_loss,b.persuasion_credit_loss,b.persuasion_contact,b.persuasion_law,b.persuasion_crime,
b.check_last_phone,b.is_refusal_time_bargain,b.is_argue,b.is_debtor_nearby,b.is_knowing,b.is_askrelation,
b.reaction_notknowing_anfu,b.reaction_notknowing_fangqi,b.reaction_notknowing_zhiwen,b.reaction_notknowing_qingqiu,	
b.check_other_call,b.mention_address,b.mention_number,b.is_provpmorigin,b.mention_photo,
b.is_askingmethod,b.is_rejection,b.mention_nocall,b.is_complaint,	
b.is_notknowing,b.mention_noconnection,	b.is_askingreduction,b.time_bargain,
b.is_helping,b.is_investigation,b.is_detail
from cszc.hsy_tongyongbianliang_M2_v2 a 
left join [shuffle] cszc.hsy_wenbenbianliang_M2 b on a.callid=b.callid
where a.callid not in (select callid from (select callid ,count(*) from cszc.hsy_wenbenbianliang_M2 group by 1 having count(*)>1) temp)
      and b.callid not in (select callid from (select callid ,count(*) from cszc.hsy_tongyongbianliang_M2 group by 1 having count(*)>1) temp);
compute stats cszc.hsy_bianliang_M2_v2;	  
	  
---------------------*分析完整生命周期（保证有刚分到案子的拨打记录）*---------------------
--5月数据缺失，所以主要处理5月数据
--处理规则，寻找同时满足is_first_yes_call='新案'，yes_call_num=1，day_num=0的记录，以这种记录为准，选取后面所有的记录
---------------------*code*---------------------	  

--观测距离分案的天数内，接通的次数
--删除10s以内
select day_num,num,count(num) as num_num	
from 	
(	
select owner_id,dun_case_id,time_mark,max(day_num) as day_num,count(*) as num 	
from 	
(	  
select a.*	
from cszc.hsy_bianliang_M2_v2 a 	  
inner join 	  
(  	
select dun_case_id,min(ringtime) as base	  
from cszc.hsy_bianliang_M2_v2	
where dt<='2018-05-31' and day_num=0 and yes_call_num=1 	
group by 1 	
) b 	
on a.dun_case_id=b.dun_case_id and a.ringtime>=b.base 	
where a.dt<='2018-05-31' and a.duration>10	
union	
select * from cszc.hsy_bianliang_M2_v2 where dt>'2018-05-31' and duration>10	
) base1 	
where time_mark<'2018-12-01'	
group by 1,2,3	
) base2 	
group by 1,2	

--观测本人
select day_num,num,count(num) as num_num	
from 	
(	
select owner_id,dun_case_id,time_mark,max(day_num) as day_num,count(*) as num 	
from 	
(	  
select a.*	
from cszc.hsy_bianliang_M2_v2 a 	  
inner join 	  
(  	
select dun_case_id,min(ringtime) as base	  
from cszc.hsy_bianliang_M2_v2	
where dt<='2018-05-31' and day_num=0 and yes_call_num=1 	
group by 1 	
) b 	
on a.dun_case_id=b.dun_case_id and a.ringtime>=b.base 	
where a.dt<='2018-05-31' 	
union	
select * from cszc.hsy_bianliang_M2_v2 where dt>'2018-05-31' and duration>10	
) base1 	
where time_mark<'2018-12-01' and contact_type='self'	
group by 1,2,3	
) base2 	
group by 1,2	


---------------------*分析完整生命周期（保证有刚分到案子的拨打记录）*---------------------
--base以上述代码为基准
--剔除10秒以下
--从一个onwer_id观测经办第一次接通电话变量的稳定性
--区分本人和联系人
--首次接通、非首次接通
--不比较处理案子在50件以下的
---------------------*code*---------------------	  
--基表
with base1 as
(	  
select a.*
from cszc.hsy_bianliang_M2_v2 a 	  
inner join 	  
(  
select dun_case_id,min(ringtime) as base	  
from cszc.hsy_bianliang_M2_v2
where dt<='2018-05-31' and day_num=0 and yes_call_num=1 
group by 1 
) b 
on a.dun_case_id=b.dun_case_id and a.ringtime>=b.base 
where a.dt<='2018-05-31' and a.duration>10
union
select * from cszc.hsy_bianliang_M2_v2 where dt>'2018-05-31' and duration>10
),
--排序，筛选在本人或者非本人接通下第一次接通的
base2 as
(
select *,
       row_number() over (partition by owner_id,dun_case_id,time_mark,new_contact_type order by ringtime) as order_num 
from 
(
select *,
       case when contact_type='self' then '逾期本人' else '联系人' end as new_contact_type
from base1 
) temp 
)
--观测联系人（第一次接通）
select
owner_id,
month(dt) as month,
count(*) as num,
avg(day_num) as day_num_mean,
avg(duration) as duration_mean,
avg(t_interaction) as t_interaction_mean,
avg(collector_speed) as collector_speed_mean,
avg(receiver_speed) as receiver_speed_mean,
avg(ratio_collector_char) as ratio_collector_char_mean,
avg(jingban_mean_sentence_num) as jingban_mean_sentence_num_mean,
avg(jingban_max_sentence_num) as jingban_max_sentence_num_mean,
avg(yonghu_mean_sentence_num) as yonghu_mean_sentence_num_mean,
avg(yonghu_max_sentence_num) as yonghu_max_sentence_num_mean,
avg(mention_repayment_time) as mention_repayment_time_mean,
avg(mention_owing_amount) as mention_owing_amount_mean,
avg(mention_default_days) as mention_default_days_mean,
avg(persuasion_money_loss) as persuasion_money_loss_mean,
avg(persuasion_credit_loss) as persuasion_credit_loss_mean,
avg(persuasion_contact) as persuasion_contact_mean,
avg(persuasion_law) as persuasion_law_mean,
avg(persuasion_crime) as persuasion_crime_mean,
avg(check_last_phone) as check_last_phone_mean,
avg(is_argue) as is_argue_mean,
avg(is_debtor_nearby) as is_debtor_nearby_mean,
avg(is_knowing) as is_knowing_mean,
avg(is_askrelation) as is_askrelation_mean,
avg(reaction_notknowing_anfu) as reaction_notknowing_anfu_mean,
avg(reaction_notknowing_fangqi) as reaction_notknowing_fangqi_mean,
avg(reaction_notknowing_zhiwen) as reaction_notknowing_zhiwen_mean,
avg(reaction_notknowing_qingqiu) as reaction_notknowing_qingqiu_mean,
avg(check_other_call) as check_other_call_mean,
avg(mention_address) as mention_address_mean,
avg(mention_number) as mention_number_mean,
avg(mention_photo) as mention_photo_mean,	   
avg(is_provpmorigin) as is_provpmorigin_mean,
avg(is_askingmethod) as is_askingmethod_mean,
avg(is_rejection) as is_rejection_mean,
avg(mention_nocall) as mention_nocall_mean,
avg(is_complaint) as is_complaint_mean,	   
avg(is_notknowing) as is_notknowing_mean,	   
avg(mention_noconnection) as mention_noconnection_mean,	     
avg(is_helping) as is_helping_mean,
avg(is_investigation) as is_investigation_mean,	   
avg(is_detail) as is_detail_mean,
stddev(day_num) as day_num_std,
stddev(duration) as duration_std,
stddev(t_interaction) as t_interaction_std,
stddev(collector_speed) as collector_speed_std,
stddev(receiver_speed) as receiver_speed_std,
stddev(ratio_collector_char) as ratio_collector_char_std,
stddev(jingban_mean_sentence_num) as jingban_mean_sentence_num_std,
stddev(jingban_max_sentence_num) as jingban_max_sentence_num_std,
stddev(yonghu_mean_sentence_num) as yonghu_mean_sentence_num_std,
stddev(yonghu_max_sentence_num) as yonghu_max_sentence_num_std,
stddev(mention_repayment_time) as mention_repayment_time_std,
stddev(mention_owing_amount) as mention_owing_amount_std,
stddev(mention_default_days) as mention_default_days_std,
stddev(persuasion_money_loss) as persuasion_money_loss_std,
stddev(persuasion_credit_loss) as persuasion_credit_loss_std,
stddev(persuasion_contact) as persuasion_contact_std,
stddev(persuasion_law) as persuasion_law_std,
stddev(persuasion_crime) as persuasion_crime_std,
stddev(check_last_phone) as check_last_phone_std,
stddev(is_argue) as is_argue_std,
stddev(is_debtor_nearby) as is_debtor_nearby_std,
stddev(is_knowing) as is_knowing_std,
stddev(is_askrelation) as is_askrelation_std,
stddev(reaction_notknowing_anfu) as reaction_notknowing_anfu_std,
stddev(reaction_notknowing_fangqi) as reaction_notknowing_fangqi_std,
stddev(reaction_notknowing_zhiwen) as reaction_notknowing_zhiwen_std,
stddev(reaction_notknowing_qingqiu) as reaction_notknowing_qingqiu_std,
stddev(check_other_call) as check_other_call_std,
stddev(mention_address) as mention_address_std,
stddev(mention_number) as mention_number_std,
stddev(mention_photo) as mention_photo_std,	   
stddev(is_provpmorigin) as is_provpmorigin_std,
stddev(is_askingmethod) as is_askingmethod_std,
stddev(is_rejection) as is_rejection_std,
stddev(mention_nocall) as mention_nocall_std,
stddev(is_complaint) as is_complaint_std,	   
stddev(is_notknowing) as is_notknowing_std,	   
stddev(mention_noconnection) as mention_noconnection_std,	     
stddev(is_helping) as is_helping_std,
stddev(is_investigation) as is_investigation_std,	   
stddev(is_detail) as is_detail_std
from 
(
select dt,owner_id,callid,contact_type,ringtime,duration,t_interaction,collector_speed,receiver_speed,day_num,
       ratio_collector_char,jingban_mean_sentence_num,jingban_max_sentence_num,yonghu_mean_sentence_num,yonghu_max_sentence_num,
	   case when mention_repayment_time='true' then 1 else 0 end as mention_repayment_time,
	   case when mention_owing_amount='true' then 1 else 0 end as mention_owing_amount,
	   case when mention_default_days='true' then 1 else 0 end as mention_default_days,
	   case when persuasion_money_loss='true' then 1 else 0 end as persuasion_money_loss,
	   case when persuasion_credit_loss='true' then 1 else 0 end as persuasion_credit_loss,
	   case when persuasion_contact='true' then 1 else 0 end as persuasion_contact,
	   case when persuasion_law='true' then 1 else 0 end as persuasion_law,
	   case when persuasion_crime='true' then 1 else 0 end as persuasion_crime,
	   case when check_last_phone='true' then 1 else 0 end as check_last_phone,
	   case when is_refusal_time_bargain='true' then 1 else 0 end as is_refusal_time_bargain,
	   case when is_argue='true' then 1 else 0 end as is_argue,
	   case when is_debtor_nearby='true' then 1 else 0 end as is_debtor_nearby,
	   case when is_knowing='true' then 1 else 0 end as is_knowing,
	   case when is_askrelation='true' then 1 else 0 end as is_askrelation,
	   case when reaction_notknowing_anfu='true' then 1 else 0 end as reaction_notknowing_anfu,
	   case when reaction_notknowing_fangqi='true' then 1 else 0 end as reaction_notknowing_fangqi,
	   case when reaction_notknowing_zhiwen='true' then 1 else 0 end as reaction_notknowing_zhiwen,
	   case when reaction_notknowing_qingqiu='true' then 1 else 0 end as reaction_notknowing_qingqiu,
	   case when check_other_call='true' then 1 else 0 end as check_other_call,
	   case when mention_address='true' then 1 else 0 end as mention_address,
	   case when mention_number='true' then 1 else 0 end as mention_number,
	   case when mention_photo='true' then 1 else 0 end as mention_photo,	   
	   case when is_provpmorigin='true' then 1 else 0 end as is_provpmorigin,
	   case when is_askingmethod='true' then 1 else 0 end as is_askingmethod,
	   case when is_rejection='true' then 1 else 0 end as is_rejection,
	   case when mention_nocall='true' then 1 else 0 end as mention_nocall,
	   case when is_complaint='true' then 1 else 0 end as is_complaint,	   
	   case when is_notknowing='true' then 1 else 0 end as is_notknowing,	   
	   case when mention_noconnection='true' then 1 else 0 end as mention_noconnection,	   
	   case when is_askingreduction='true' then 1 else 0 end as is_askingreduction,	   
	   case when time_bargain='true' then 1 else 0 end as time_bargain,	   
	   case when is_helping='true' then 1 else 0 end as is_helping,
	   case when is_investigation='true' then 1 else 0 end as is_investigation,	   
	   case when is_detail='true' then 1 else 0 end as is_detail
from base2	   
where new_contact_type='联系人' and order_num=1
) temp
group by 1,2 having count(*)>50

--观测本人（第一次接通）
select
owner_id,
month(dt) as month,
count(*) as num,
avg(day_num) as day_num_mean,
avg(duration) as duration_mean,
avg(t_interaction) as t_interaction_mean,
avg(collector_speed) as collector_speed_mean,
avg(receiver_speed) as receiver_speed_mean,
avg(ratio_collector_char) as ratio_collector_char_mean,
avg(jingban_mean_sentence_num) as jingban_mean_sentence_num_mean,
avg(jingban_max_sentence_num) as jingban_max_sentence_num_mean,
avg(yonghu_mean_sentence_num) as yonghu_mean_sentence_num_mean,
avg(yonghu_max_sentence_num) as yonghu_max_sentence_num_mean,
avg(mention_repayment_time) as mention_repayment_time_mean,
avg(mention_owing_amount) as mention_owing_amount_mean,
avg(mention_default_days) as mention_default_days_mean,
avg(persuasion_money_loss) as persuasion_money_loss_mean,
avg(persuasion_credit_loss) as persuasion_credit_loss_mean,
avg(persuasion_contact) as persuasion_contact_mean,
avg(persuasion_law) as persuasion_law_mean,
avg(persuasion_crime) as persuasion_crime_mean,
avg(check_last_phone) as check_last_phone_mean,
avg(is_argue) as is_argue_mean,
avg(is_askrelation) as is_askrelation_mean,
avg(check_other_call) as check_other_call_mean,
avg(mention_address) as mention_address_mean,
avg(mention_number) as mention_number_mean,
avg(mention_photo) as mention_photo_mean,	   
avg(is_provpmorigin) as is_provpmorigin_mean,
avg(is_askingmethod) as is_askingmethod_mean,
avg(is_rejection) as is_rejection_mean,
avg(mention_nocall) as mention_nocall_mean,
avg(is_complaint) as is_complaint_mean,	        
avg(is_helping) as is_helping_mean,
avg(is_refusal_time_bargain) as is_refusal_time_bargain_mean,
avg(time_bargain) as time_bargain_mean,
avg(is_askingreduction) as is_askingreduction_mean,
stddev(day_num) as day_num_std,
stddev(duration) as duration_std,
stddev(t_interaction) as t_interaction_std,
stddev(collector_speed) as collector_speed_std,
stddev(receiver_speed) as receiver_speed_std,
stddev(ratio_collector_char) as ratio_collector_char_std,
stddev(jingban_mean_sentence_num) as jingban_mean_sentence_num_std,
stddev(jingban_max_sentence_num) as jingban_max_sentence_num_std,
stddev(yonghu_mean_sentence_num) as yonghu_mean_sentence_num_std,
stddev(yonghu_max_sentence_num) as yonghu_max_sentence_num_std,
stddev(mention_repayment_time) as mention_repayment_time_std,
stddev(mention_owing_amount) as mention_owing_amount_std,
stddev(mention_default_days) as mention_default_days_std,
stddev(persuasion_money_loss) as persuasion_money_loss_std,
stddev(persuasion_credit_loss) as persuasion_credit_loss_std,
stddev(persuasion_contact) as persuasion_contact_std,
stddev(persuasion_law) as persuasion_law_std,
stddev(persuasion_crime) as persuasion_crime_std,
stddev(check_last_phone) as check_last_phone_std,
stddev(is_argue) as is_argue_std,
stddev(is_askrelation) as is_askrelation_std,
stddev(check_other_call) as check_other_call_std,
stddev(mention_address) as mention_address_std,
stddev(mention_number) as mention_number_std,
stddev(mention_photo) as mention_photo_std,	   
stddev(is_provpmorigin) as is_provpmorigin_std,
stddev(is_askingmethod) as is_askingmethod_std,
stddev(is_rejection) as is_rejection_std,
stddev(mention_nocall) as mention_nocall_std,
stddev(is_complaint) as is_complaint_std,    
stddev(is_helping) as is_helping_std,
stddev(is_refusal_time_bargain) as is_refusal_time_bargain_std,
stddev(time_bargain) as time_bargain_std,
stddev(is_askingreduction) as is_askingreduction_std
from 
(
select dt,owner_id,callid,contact_type,ringtime,duration,t_interaction,collector_speed,receiver_speed,day_num,
       ratio_collector_char,jingban_mean_sentence_num,jingban_max_sentence_num,yonghu_mean_sentence_num,yonghu_max_sentence_num,
	   case when mention_repayment_time='true' then 1 else 0 end as mention_repayment_time,
	   case when mention_owing_amount='true' then 1 else 0 end as mention_owing_amount,
	   case when mention_default_days='true' then 1 else 0 end as mention_default_days,
	   case when persuasion_money_loss='true' then 1 else 0 end as persuasion_money_loss,
	   case when persuasion_credit_loss='true' then 1 else 0 end as persuasion_credit_loss,
	   case when persuasion_contact='true' then 1 else 0 end as persuasion_contact,
	   case when persuasion_law='true' then 1 else 0 end as persuasion_law,
	   case when persuasion_crime='true' then 1 else 0 end as persuasion_crime,
	   case when check_last_phone='true' then 1 else 0 end as check_last_phone,
	   case when is_refusal_time_bargain='true' then 1 else 0 end as is_refusal_time_bargain,
	   case when is_argue='true' then 1 else 0 end as is_argue,
	   case when is_debtor_nearby='true' then 1 else 0 end as is_debtor_nearby,
	   case when is_knowing='true' then 1 else 0 end as is_knowing,
	   case when is_askrelation='true' then 1 else 0 end as is_askrelation,
	   case when reaction_notknowing_anfu='true' then 1 else 0 end as reaction_notknowing_anfu,
	   case when reaction_notknowing_fangqi='true' then 1 else 0 end as reaction_notknowing_fangqi,
	   case when reaction_notknowing_zhiwen='true' then 1 else 0 end as reaction_notknowing_zhiwen,
	   case when reaction_notknowing_qingqiu='true' then 1 else 0 end as reaction_notknowing_qingqiu,
	   case when check_other_call='true' then 1 else 0 end as check_other_call,
	   case when mention_address='true' then 1 else 0 end as mention_address,
	   case when mention_number='true' then 1 else 0 end as mention_number,
	   case when mention_photo='true' then 1 else 0 end as mention_photo,	   
	   case when is_provpmorigin='true' then 1 else 0 end as is_provpmorigin,
	   case when is_askingmethod='true' then 1 else 0 end as is_askingmethod,
	   case when is_rejection='true' then 1 else 0 end as is_rejection,
	   case when mention_nocall='true' then 1 else 0 end as mention_nocall,
	   case when is_complaint='true' then 1 else 0 end as is_complaint,	   
	   case when is_notknowing='true' then 1 else 0 end as is_notknowing,	   
	   case when mention_noconnection='true' then 1 else 0 end as mention_noconnection,	   
	   case when is_askingreduction='true' then 1 else 0 end as is_askingreduction,	   
	   case when time_bargain='true' then 1 else 0 end as time_bargain,	   
	   case when is_helping='true' then 1 else 0 end as is_helping,
	   case when is_investigation='true' then 1 else 0 end as is_investigation,	   
	   case when is_detail='true' then 1 else 0 end as is_detail
from base2	   
where new_contact_type='逾期本人' and order_num=1
) temp
group by 1,2 having count(*)>50



select
owner_id,
month(dt) as month,
count(*) as num,
avg(day_num) as day_num_mean,
avg(duration) as duration_mean,
avg(t_interaction) as t_interaction_mean,
avg(collector_speed) as collector_speed_mean,
avg(receiver_speed) as receiver_speed_mean,
avg(ratio_collector_char) as ratio_collector_char_mean,
avg(jingban_mean_sentence_num) as jingban_mean_sentence_num_mean,
avg(jingban_max_sentence_num) as jingban_max_sentence_num_mean,
avg(yonghu_mean_sentence_num) as yonghu_mean_sentence_num_mean,
avg(yonghu_max_sentence_num) as yonghu_max_sentence_num_mean,
avg(mention_repayment_time) as mention_repayment_time_mean,
avg(mention_owing_amount) as mention_owing_amount_mean,
avg(mention_default_days) as mention_default_days_mean,
avg(persuasion_money_loss) as persuasion_money_loss_mean,
avg(persuasion_credit_loss) as persuasion_credit_loss_mean,
avg(persuasion_contact) as persuasion_contact_mean,
avg(persuasion_law) as persuasion_law_mean,
avg(persuasion_crime) as persuasion_crime_mean,
avg(check_last_phone) as check_last_phone_mean,
avg(is_refusal_time_bargain) as is_refusal_time_bargain_mean,
avg(is_argue) as is_argue_mean,
avg(is_askrelation) as is_askrelation_mean,
avg(check_other_call) as check_other_call_mean,
avg(mention_address) as mention_address_mean,
avg(mention_number) as mention_number_mean,
avg(mention_photo) as mention_photo_mean,	   
avg(is_provpmorigin) as is_provpmorigin_mean,
avg(is_askingmethod) as is_askingmethod_mean,
avg(is_rejection) as is_rejection_mean,
avg(mention_nocall) as mention_nocall_mean,
avg(is_complaint) as is_complaint_mean,	        
avg(is_askingreduction) as is_askingreduction_mean,	   
avg(time_bargain) as time_bargain_mean,	   
avg(is_helping) as is_helping_mean
from 
(
select dt,owner_id,callid,contact_type,ringtime,duration,t_interaction,collector_speed,receiver_speed,day_num,
       ratio_collector_char,jingban_mean_sentence_num,jingban_max_sentence_num,yonghu_mean_sentence_num,yonghu_max_sentence_num,
	   case when mention_repayment_time='true' then 1 else 0 end as mention_repayment_time,
	   case when mention_owing_amount='true' then 1 else 0 end as mention_owing_amount,
	   case when mention_default_days='true' then 1 else 0 end as mention_default_days,
	   case when persuasion_money_loss='true' then 1 else 0 end as persuasion_money_loss,
	   case when persuasion_credit_loss='true' then 1 else 0 end as persuasion_credit_loss,
	   case when persuasion_contact='true' then 1 else 0 end as persuasion_contact,
	   case when persuasion_law='true' then 1 else 0 end as persuasion_law,
	   case when persuasion_crime='true' then 1 else 0 end as persuasion_crime,
	   case when check_last_phone='true' then 1 else 0 end as check_last_phone,
	   case when is_refusal_time_bargain='true' then 1 else 0 end as is_refusal_time_bargain,
	   case when is_argue='true' then 1 else 0 end as is_argue,
	   case when is_debtor_nearby='true' then 1 else 0 end as is_debtor_nearby,
	   case when is_knowing='true' then 1 else 0 end as is_knowing,
	   case when is_askrelation='true' then 1 else 0 end as is_askrelation,
	   case when reaction_notknowing_anfu='true' then 1 else 0 end as reaction_notknowing_anfu,
	   case when reaction_notknowing_fangqi='true' then 1 else 0 end as reaction_notknowing_fangqi,
	   case when reaction_notknowing_zhiwen='true' then 1 else 0 end as reaction_notknowing_zhiwen,
	   case when reaction_notknowing_qingqiu='true' then 1 else 0 end as reaction_notknowing_qingqiu,
	   case when check_other_call='true' then 1 else 0 end as check_other_call,
	   case when mention_address='true' then 1 else 0 end as mention_address,
	   case when mention_number='true' then 1 else 0 end as mention_number,
	   case when mention_photo='true' then 1 else 0 end as mention_photo,	   
	   case when is_provpmorigin='true' then 1 else 0 end as is_provpmorigin,
	   case when is_askingmethod='true' then 1 else 0 end as is_askingmethod,
	   case when is_rejection='true' then 1 else 0 end as is_rejection,
	   case when mention_nocall='true' then 1 else 0 end as mention_nocall,
	   case when is_complaint='true' then 1 else 0 end as is_complaint,	   
	   case when is_notknowing='true' then 1 else 0 end as is_notknowing,	   
	   case when mention_noconnection='true' then 1 else 0 end as mention_noconnection,	   
	   case when is_askingreduction='true' then 1 else 0 end as is_askingreduction,	   
	   case when time_bargain='true' then 1 else 0 end as time_bargain,	   
	   case when is_helping='true' then 1 else 0 end as is_helping,
	   case when is_investigation='true' then 1 else 0 end as is_investigation,	   
	   case when is_detail='true' then 1 else 0 end as is_detail
from base2	   
) temp
where new_contact_type='逾期本人'
group by 1 having count(*)>50






	  
---------------------*分析新案即第一次拨打的情况*---------------------
--5月数据缺失，所以要满足两个条件，is_first_yes_call='新案'，yes_call_num=1，day_num=0
--6月后数据算是完整，可以只满足一个条件yes_call_num=1
--剔除通话不足10秒
---------------------*code*---------------------	  
	  
	  
	  
'''	  
--sql合并生成pattern	  
select callid,contact_type,channel,seq_id,content,
       concat(mention_repayment_time,mention_owing_amount,mention_default_days,persuasion_money_loss,
	                persuasion_credit_loss,persuasion_contact,persuasion_law,persuasion_crime,check_last_phone,
					is_refusal_time_bargain,is_argue,is_debtor_nearby,is_knowing,is_askrelation,reaction_notknowing_anfu,
					reaction_notknowing_fangqi,reaction_notknowing_zhiwen,reaction_notknowing_qingqiu) as jingban_pattern
from 
(
select callid,contact_type,channel,seq_id,content,
case when mention_repayment_time='false' or mention_repayment_time is null then "" else mention_repayment_time end as mention_repayment_time,
case when mention_owing_amount='false' or mention_owing_amount is null then "" else mention_owing_amount end as mention_owing_amount,
case when mention_default_days='false' or mention_default_days is null then "" else mention_default_days end as mention_default_days,
case when persuasion_money_loss='false' or persuasion_money_loss is null then "" else persuasion_money_loss end as persuasion_money_loss,
case when persuasion_credit_loss='false' or persuasion_credit_loss is null then "" else persuasion_credit_loss end as persuasion_credit_loss,
case when persuasion_contact='false' or persuasion_contact is null then "" else persuasion_contact end as persuasion_contact,
case when persuasion_law='false' or persuasion_law is null then "" else persuasion_law end as persuasion_law,
case when persuasion_crime='false' or persuasion_crime is null then "" else persuasion_crime end as persuasion_crime,
case when check_last_phone='false' or check_last_phone is null then "" else check_last_phone end as check_last_phone,
case when is_refusal_time_bargain='false' or is_refusal_time_bargain is Null then "" else is_refusal_time_bargain end as is_refusal_time_bargain,
case when is_argue='false' or is_argue is null then "" else is_argue end as is_argue,
case when is_debtor_nearby='false' or is_debtor_nearby is null then "" else is_debtor_nearby end as is_debtor_nearby,
case when is_knowing='false' or is_knowing is null then "" else is_knowing end as is_knowing,
case when is_askrelation='false' or is_askrelation is null then "" else is_askrelation end as is_askrelation,
case when reaction_notknowing_anfu='false' or reaction_notknowing_anfu is null then "" else reaction_notknowing_anfu end as reaction_notknowing_anfu,
case when reaction_notknowing_fangqi='false' or reaction_notknowing_fangqi is null then "" else reaction_notknowing_fangqi end as reaction_notknowing_fangqi,
case when reaction_notknowing_zhiwen='false' or reaction_notknowing_zhiwen is null then "" else reaction_notknowing_zhiwen end as reaction_notknowing_zhiwen,
case when reaction_notknowing_qingqiu='false' or reaction_notknowing_qingqiu is null then "" else reaction_notknowing_qingqiu end as reaction_notknowing_qingqiu
from cszc.hsy_jingbanxingwei_M2	  
) temp;

select callid,contact_type,channel,seq_id,content,
       concat(is_askingmethod,is_rejection,mention_nocall,is_askingreduction,time_bargain,is_notknowing,mention_noconnection,is_complaint)	  	  
from 
(	  
select callid,contact_type,channel,seq_id,content,
case when is_askingmethod='false' or is_askingmethod is null then "" else is_askingmethod end as is_askingmethod,
case when is_rejection='false' or is_rejection is null then "" else is_rejection end as is_rejection,
case when mention_nocall='false' or mention_nocall is null then "" else mention_nocall end as mention_nocall,
case when is_askingreduction='false' or is_askingreduction is null then "" else is_askingreduction end as is_askingreduction,
case when time_bargain='false' or time_bargain is null then "" else time_bargain end as time_bargain,
case when is_notknowing='false' or is_notknowing is null then "" else is_notknowing end as is_notknowing,
case when mention_noconnection='false' or mention_noconnection is null then "" else mention_noconnection end as mention_noconnection,
case when is_complaint='false' or is_complaint is null then "" else is_complaint end as is_complaint
from cszc.hsy_yonghuxingwei_M2	  
) temp
'''