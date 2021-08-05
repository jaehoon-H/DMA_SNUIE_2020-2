import whoosh.index as index
from whoosh.qparser import QueryParser, OrGroup
from whoosh import scoring
import CustomScoring as scoring
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.stem import SnowballStemmer
from nltk.stem import LancasterStemmer
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer


def getSearchEngineResult(query_dict):
    result_dict = {}
    ix = index.open_dir("index")

    with ix.searcher(weighting=scoring.ScoringFunction()) as searcher:
    # with ix.searcher(weighting=scoring.ScoringFunction()) as searcher:
        # TODO - Define your own query parser
        parser = QueryParser("contents", schema=ix.schema, group=OrGroup.factory(0.999))
        stopWords = set(stopwords.words('english'))
        n = WordNetLemmatizer()
        #s1 = PorterStemmer()
        #s2 = SnowballStemmer('english')
        s3 = LancasterStemmer()

        # print(stopWords)
        for qid, q in query_dict.items():
            table = str.maketrans('\n?.,!', '     ')
            q = q.translate(table)
            new_q = ''
            for word in q.split(' '):
            #for word in q.split(' '):
                if word.lower() not in stopWords:

                    word = n.lemmatize(word.lower())
                    #new_q += word + ' '
                    new_q += s3.stem(word) + ' '
                    '''
                    syns = wordnet.synsets(s3.stem(word).lower())
                    if len(syns)>4:
                        #new_q += syns[].lemmas()[0].name() + ' '
                        #new_q += syns[2].lemmas()[0].name() + ' '
                        new_q += s3.stem(n.lemmatize(syns[1].lemmas()[0].name())) + ' '
                        new_q += s3.stem(n.lemmatize(syns[2].lemmas()[0].name())) + ' '
                        new_q += s3.stem(n.lemmatize(syns[3].lemmas()[0].name())) + ' '
                        '''

            #print(new_q)


            #print(new_q)
            query = parser.parse(new_q.lower())
            results = searcher.search(query, limit=None)
            #for result in results:
             #   print(result.fields()['docID'], result.score)

            result_dict[qid] = [result.fields()['docID'] for result in results]

    return result_dict