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

for filename in files[:1]:
    df = pd.read_csv(filename)

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

    for idx, topic in enumerate(lda.components_):
        wordDist = sorted([(i, v) for i, v in enumerate(topic)], key=lambda x: x[1], reverse=True)
        top10 = ' + ' .join([str(value) + '*' + featureNames[i] for (i, value) in wordDist[:10]])
        tbs.append((ownerid, top10))

    pbar.update()

    # pyLDAvis.enable_notebook(local=True)
    pyLDAvis.sklearn.prepare(lda, cntTf, cntVector)
    data = pyLDAvis.sklearn.prepare(lda, cntTf, cntVector)
    pyLDAvis.show(data, open_browser=True, ip='172.20.7.199', port=8888)

df_all = pd.DataFrame(tbs, columns=['ownerid', 'topic'])
df_all.to_csv('./results/allWithoutScore.csv', index=False, encoding='utf8')
print(df_all.head())
