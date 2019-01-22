import os
import warnings
warnings.filterwarnings('ignore')

import pyLDAvis.sklearn
import pyprind
import jieba
import pandas as pd
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer

from impala.dbapi import connect
from impala.util import as_pandas
from config import impala_settings
conn = connect(**impala_settings)
cur = conn.cursor()

def top10_last10():
    """得到绩效前20 和后20的绩效"""
    sql = r"""select owner_id
        from
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
    """
    cur.execute(sql)
    return as_pandas(cur)


base_dir = './dataExceptSelf/'

df_ownerIds = top10_last10()

files = sorted([base_dir + file for file in os.listdir(base_dir) if file.endswith('.csv')
                if int(file.split('.')[0]) in df_ownerIds['owner_id'].values])

pbar = pyprind.ProgBar(len(files))
tbs = []

for filename in files:
    df = pd.read_csv(filename)
    df = df.loc[(df['phone_type'] != 'self') & (df['duration'] >= 10)]
    df.reset_index(drop=True, inplace=True)

    suggest_words = ['拍拍贷', '上海拍拍贷', '合肥拍拍贷', '长沙拍拍贷', '拍拍贷法务部',
                     '打个电话', '打电话', '上海拍拍贷法务部']

    for word in suggest_words:
        jieba.suggest_freq(word, True)

    df['content_cut_words'] = df['content'].map(lambda s: ' '.join(jieba.cut(s)))

    stopwords = []
    for word in open('stopwords.txt', encoding='utf8', mode='r'):
        stopwords.append(word.strip())

    corpus = df['content_cut_words'].values

    n_features = 1000
    cntVector = CountVectorizer(strip_accents='unicode',
                                stop_words=stopwords,
                                max_features=n_features)
    cntTf = cntVector.fit_transform(corpus)
    featureNames = cntVector.get_feature_names()

    lda = LatentDirichletAllocation(n_components=2,
                                    learning_offset=50.,
                                    random_state=0,
                                    learning_method='batch')
    docres = lda.fit_transform(cntTf)
    ownerid = filename.split('/')[-1].split('.')[0]
    df_theme_dist = pd.DataFrame(docres, columns=['topic0_prob', 'topic1_prob'])
    # df_theme_dist['ownid'] = ownerid
    df_new = pd.concat([df, df_theme_dist], axis=1)

    for idx, topic in enumerate(lda.components_):
        wordDist = sorted([(i, v) for i, v in enumerate(topic)], key=lambda x: x[1], reverse=True)
        top10 = ',' .join([featureNames[i] for (i, value) in wordDist[:10]])
        topic = 'topic%s' % idx
        df_new[topic] = top10

    tbs.append(df_new)
    pbar.update()

df_all = pd.concat(tbs, ignore_index=True)
df_all.to_excel('results/themeDistOnEachAgent20190122.xlsx', index=False, encoding='utf8')
