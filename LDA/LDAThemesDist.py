import os
import warnings
import pyLDAvis.sklearn
import pyprind
import jieba
import pandas as pd
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer

warnings.filterwarnings('ignore')

base_dir = './data/'
files = sorted([base_dir + file for file in os.listdir(base_dir) if file.endswith('.csv')])

pbar = pyprind.ProgBar(len(files))
tbs = []

for filename in files:
    df = pd.read_csv(filename)

    suggest_words = ['拍拍贷', '上海拍拍贷', '合肥拍拍贷', '长沙拍拍贷', '拍拍贷法务部']
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
    df_theme_dist = pd.DataFrame(docres, columns=['topic0', 'topic1'])
    df_theme_dist['ownid'] = ownerid
    tbs.append(df_theme_dist)
    pbar.update()

df_all = pd.concat(tbs, ignore_index=True)
df_all.to_csv('results/themeDistOnEachAgent.csv', index=False, encoding='utf8')
