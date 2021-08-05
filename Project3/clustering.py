from sklearn.datasets import load_files
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn import metrics
from sklearn.cluster import KMeans
from nltk.corpus import stopwords
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import Normalizer
from sklearn.pipeline import make_pipeline

categories = ['Business', 'Entertainment', 'Living', 'Metro', 'Shopping', 'Sports', 'Tech']

data = load_files(container_path='C:/Users/User/Downloads/DMA_project3/DMA_project3/CC/text_all', categories=categories, shuffle=True,
                    encoding='utf-8', decode_error='replace')

# TODO - Data preprocessing and clustering
stop_words=set(stopwords.words('english'))
count_vec=CountVectorizer(stop_words=stop_words,min_df=10, max_df=600)
vec_data =count_vec.fit_transform(data.data)
data_tf  =TfidfTransformer().fit_transform(vec_data)

svd = TruncatedSVD(n_components=100, random_state=44)
normalizer = Normalizer(copy=False)
lsa = make_pipeline(svd,normalizer)
data_lsa=lsa.fit_transform(data_tf)
clst = KMeans(n_clusters=7, random_state=0)
clst.fit(data_lsa)


from sklearn.datasets import load_files
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn import metrics
from sklearn.cluster import KMeans
from nltk.corpus import stopwords
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import Normalizer
from sklearn.pipeline import make_pipeline

categories = ['Business', 'Entertainment', 'Living', 'Metro', 'Shopping', 'Sports', 'Tech']

data = load_files(container_path='C:/Users/User/Downloads/DMA_project3/DMA_project3/CC/text_all', categories=categories, shuffle=True,
                    encoding='utf-8', decode_error='replace')

# TODO - Data preprocessing and clustering
stop_words=set(stopwords.words('english'))
count_vec=CountVectorizer(stop_words=stop_words,min_df=10, max_df=600)
vec_data =count_vec.fit_transform(data.data)
data_tf  =TfidfTransformer().fit_transform(vec_data)

svd = TruncatedSVD(n_components=100, random_state=44)
normalizer = Normalizer(copy=False)
lsa = make_pipeline(svd,normalizer)
data_lsa=lsa.fit_transform(data_tf)
clst = KMeans(n_clusters=7, random_state=0)
clst.fit(data_lsa)
print(metrics.v_measure_score(data.target, clst.labels_))