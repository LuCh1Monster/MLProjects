# data 文件夹

- 文件名: 经办ID + '.csv'
- 每一个文件存放的是一个经办 5月份的通话数据
    - 不包含用户的通话数据
    - 包含了本人和联系人的通话数据

# results 文件夹

**当分成2类主题时:**

- all.csv : 每个经办具体的两个主题是什么
- themeDistOnEachAgent.csv : 每个经办的每通电话的所属的主题概率

> 样例数据(themeDistOnEachAgent.csv): 

```bash
topic0,topic1,ownerid
0.027 ,0.973 ,*
0.593 ,0.407 ,*
```

- 主题分布.xlsx

```bash
ownerid	belong	topic0	totalPhoneCallNum	proportion	proportion
*	0	123	1303	0.094397544	0.094 
*	1	904	1303	0.693783576	0.694 
*	2	276	1303	0.21181888	0.212 
```