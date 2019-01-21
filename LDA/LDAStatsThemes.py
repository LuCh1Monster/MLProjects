import pandas as pd

filename = './results/themeDistOnEachAgent.csv'
df = pd.read_csv(filename)

# 统计每个人有多少通话数量
df_wc = df.pivot_table(index=['ownerid'],
                        values=['topic0'],
                        aggfunc='count').reset_index()
df_wc.rename(columns={'topic0': 'totalPhoneCallNum'}, inplace=True)

# 说明: 以 0.3 0.7 切三个 Bin
## <= 0.3 不属于这个主题; 介于 0.3~0.7 之间的属于两个主图; 大于 0.7 的为该主题
## 0 表示主题一, 1 表示主题二，2 表示两个主题都有
f_cate = lambda s: 1 if s <= 0.3 else (0 if s >= 0.7 else 2)
df['belong'] = df['topic0'].map(f_cate)
df_pvt = df.pivot_table(index=['ownerid', 'belong'],
                        values=['topic0'],
                        aggfunc='count').reset_index()
df_pvt = df_pvt.merge(df_wc, on=['ownerid'])
df_pvt['proportion'] = df_pvt['topic0'] / df_pvt['totalPhoneCallNum']
df_pvt.to_excel('./results/主题分布.xlsx', index=False, encoding='utf8')
