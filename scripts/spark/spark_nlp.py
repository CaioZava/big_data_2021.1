import pandas as pd
import sparknlp
spark = sparknlp.start()

from pyspark import SparkContext
sc =SparkContext.getOrCreate()
from pyspark.sql import SQLContext
sql = SQLContext(sc)

from pyspark.sql import functions as F
from pyspark.sql.types import *

from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sparkdf = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

customSchema = StructType([
    StructField("id", StringType(), True), StructField("created_at", StringType(), False), StructField("date", StringType(), False)
    , StructField("tweet", StringType(), False), StructField("user_id", StringType(), False), StructField("user_id_str", StringType(), False)
    , StructField("username", StringType(), False), StructField("nlikes", StringType(), False), StructField("nretweets", StringType(), False)
])

spar_data_ip = sparkdf.createDataFrame(df,schema=customSchema)

# saving the df to disk
spar_data_ip.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/tangr/output.csv')

# SparkNLP Pipeline built + Sentiment Analysis out

# Function i/p: data_ip=spark data frame, input_col=name of raw text column, import_c=whether to import all the elements of pipline or not (keep true for first run only)

data_ip = spar_data_ip
input_col = 'tweet'
import_c=True
def _clean_sent_pipeline(data_ip, input_col, import_c=True):
    print(f"\t\t\t---- Starting the pipeline built for >>> {input_col} <<< with import condition {import_c} ----")
    from pyspark.sql import functions as F
    data = data_ip
    from pyspark.sql.types import IntegerType
    data = data.withColumn("id", data["id"].cast(IntegerType()))
    text_col = input_col
    non_null_index = (data.filter(data[text_col].isNotNull())).select('id')

    text_clean = data.select(text_col).filter(F.col(text_col).isNotNull())
    print(f"\n\t1. Cleaning the input for Null {data.count()} to {data.count() - non_null_index.count()}")

    if import_c: from sparknlp.base import DocumentAssembler
    documentAssembler = DocumentAssembler().setInputCol(text_col).setOutputCol('document')
    print(f"\n\t2. Attaching DocumentAssembler Transformer to the pipeline")

    if import_c: from sparknlp.annotator import Tokenizer
    tokenizer = Tokenizer().setInputCols(['document']).setOutputCol('tokenized')
    print(f"\n\t3. Attaching Tokenizer Annotator to the pipeline")

    if import_c: from sparknlp.annotator import Normalizer
    normalizer = Normalizer().setInputCols(['tokenized']).setOutputCol('normalized').setLowercase(
        True)
    print(f"\n\t4. Attaching Normalizer Annotator to the pipeline")

    if import_c: from sparknlp.annotator import LemmatizerModel
    lemmatizer = sparknlp.annotator.LemmatizerModel.pretrained().setInputCols(['normalized']).setOutputCol('lemmatized')
    print(f"\n\t5. Attaching LemmatizerModel Annotator to the pipeline")

    if import_c:
        import nltk
        nltk.download("popular")
    from nltk.corpus import stopwords
    eng_stopwords = stopwords.words('english')
    print(f"\n\t6. nltk stop-words found")

    if import_c: from sparknlp.annotator import StopWordsCleaner
    stopwords_cleaner = StopWordsCleaner().setInputCols(['lemmatized']).setOutputCol(
        'unigrams').setStopWords(eng_stopwords)
    print(f"\n\t7. Attaching StopWordsCleaner Annotator to the pipeline")

    if import_c: from sparknlp.annotator import NGramGenerator
    ngrammer = NGramGenerator().setInputCols(['lemmatized']).setOutputCol('ngrams').setN(
        3).setEnableCumulative(True).setDelimiter('_')
    print(f"\n\t8. Attaching NGramGenerator Annotator to the pipeline")

    if import_c: from sparknlp.annotator import PerceptronModel
    pos_tagger = PerceptronModel.pretrained('pos_anc').setInputCols(
        ['document', 'lemmatized']).setOutputCol('pos')
    print(f"\n\t9. Attaching PerceptronModel Annotator to the pipeline")

    if import_c: from sparknlp.base import Finisher
    finisher = Finisher().setInputCols(['unigrams', 'ngrams', 'pos'])
    print(f"\n\t10. Attaching Finisher Transformer to the pipeline")

    from pyspark.ml import Pipeline
    pipeline = Pipeline().setStages([documentAssembler,
                                     tokenizer,
                                     normalizer,
                                     lemmatizer,
                                     stopwords_cleaner,
                                     pos_tagger,
                                     ngrammer,
                                     finisher])
    print("\n\t\t\t ---- Pipeline Built Successfully ----")

    processed_tweets = pipeline.fit(text_clean).transform(text_clean)
    print("\n\t\t\t ---- Pipeline Fitted Successfully ----")

    from pyspark.sql.functions import concat
    processed_tweets = processed_tweets.withColumn('final',
                                                   concat(F.col('finished_unigrams'), F.col('finished_ngrams')))
    print("\n\tData Concatination done - uni--ngrams")

    print("\n\t\t\t ---- Loading the Pre-trained Pipeline  analyze_sentimentdl_use_twitter----")

    from sparknlp.pretrained import PretrainedPipeline
    pipeline_sent = PretrainedPipeline("analyze_sentimentdl_use_twitter", lang="en")

    pipout_sent_results = pipeline_sent.transform(processed_tweets.withColumnRenamed(text_col, "text"))

    print("\n\t\t\t ---- Sentiments Fetched Successfully ----\n\n\n")

    from pyspark.sql.functions import col
    from pyspark.sql.functions import monotonically_increasing_id, row_number
    from pyspark.sql.window import Window
    pipout_sent_results = pipout_sent_results.withColumn("id_tmp", row_number().over(
        Window.orderBy(monotonically_increasing_id())))
    non_null_index = non_null_index.withColumn("id_tmp",
                                               row_number().over(Window.orderBy(monotonically_increasing_id())))

    print("\n$$$ Indexing done for the Compiled Result")

    data_op = data.join(non_null_index.join(pipout_sent_results, on=["id_tmp"]).drop("id_tmp"), on=["id"],
                        how='left_outer')
    data_op = data_op.withColumn("id", data_op["id"].cast(IntegerType()))

    print("\n$$$ Joining the final resutls with original dataframe")  # fuck<<catch this

    print(
        f"\nOriginal IP={data.count()} \nNonNull Index={non_null_index.count()} \nNull_Clean={text_clean.count()} \nOriginal OP={data_op.count()}")
    print(data.show(4))
    # print("\t\t\t\t\t CONVERTED TO THIS")
    final_results = data_op.orderBy("id")
    print("\n$$$ Spark Created")

    id = list((((final_results.select('id')).toPandas())).id)
    createdat = list((((final_results.select('created_at')).toPandas())).created_at)
    fulltext = list((((final_results.select('tweet')).toPandas())).tweet)
    favoritecount = list((((final_results.select('nlikes')).toPandas())).nlikes)
    retweetcount = list((((final_results.select('nretweets')).toPandas())).nretweets)
    pipeclean = list((((final_results.select('text')).toPandas())).text)
    textlen = list(
        ((final_results.select('finished_unigrams')).toPandas()).finished_unigrams.apply(lambda row: int(len(row))))
    sentscores = list(((final_results.select('sentiment')).toPandas()).sentiment.apply(
        lambda row: (((str(row)).split(",")[3]).split("'")[1])))
    #op_df = pd.DataFrame(list(zip(id, createdat, fulltext, favoritecount, retweetcount, pipeclean, textlen, sentscores)),
    #                    columns=['id', 'created_at', 'tweet', 'nlikes', 'nretweets',
    #                             'text_pipe_clean', 'text_length', 'sentiment_score'])
    op_df = pd.DataFrame(
        list(zip(id, createdat, fulltext, favoritecount, retweetcount, pipeclean)),
        columns=['id', 'created_at', 'tweet', 'nlikes', 'nretweets',
                 'text_pipe_clean'])

    print("\n$$$ Pandas Created")
    print(op_df.head(4))

    return op_df

ipcol = input_col
def LDA_pipefit (data_ip, ipcol):
  text_col = ipcol
  from sparknlp.base import DocumentAssembler
  documentAssembler = DocumentAssembler().setInputCol(text_col).setOutputCol('document')
  from sparknlp.annotator import Tokenizer
  tokenizer = Tokenizer().setInputCols(['document']).setOutputCol('tokenized')
  from sparknlp.annotator import Normalizer
  normalizer = Normalizer().setInputCols(['tokenized']).setOutputCol('normalized').setLowercase(True)
  from sparknlp.annotator import LemmatizerModel
  lemmatizer = LemmatizerModel.pretrained().setInputCols(['normalized']).setOutputCol('lemmatized')
  from sparknlp.annotator import StopWordsCleaner
  stopwords_cleaner = StopWordsCleaner().setInputCols(['lemmatized']).setOutputCol('unigrams').setStopWords(eng_stopwords)
  from sparknlp.annotator import NGramGenerator
  ngrammer = NGramGenerator().setInputCols(['lemmatized']).setOutputCol('ngrams').setN(3).setEnableCumulative(True).setDelimiter('_')
  from sparknlp.annotator import PerceptronModel
  pos_tagger = PerceptronModel.pretrained('pos_anc').setInputCols(['document', 'lemmatized']).setOutputCol('pos')
  from sparknlp.base import Finisher
  finisher = Finisher().setInputCols(['unigrams', 'ngrams','pos'])
  from pyspark.ml import Pipeline
  pipeline = Pipeline().setStages([documentAssembler,
                                  tokenizer,
                                  normalizer,
                                  lemmatizer,
                                  stopwords_cleaner,
                                  pos_tagger,
                                  ngrammer,
                                  finisher])
  review_text_clean = ipcol
  processed_tweets = pipeline.fit(data_ip).transform(data_ip)
  from pyspark.sql.functions import concat
  processed_tweets = processed_tweets.withColumn('final',concat(F.col('finished_unigrams'), F.col('finished_ngrams')))
  from pyspark.ml.feature import CountVectorizer
  tfizer = CountVectorizer(inputCol='final',outputCol='tf_features')
  tf_model = tfizer.fit(processed_tweets)
  tf_result = tf_model.transform(processed_tweets)
  from pyspark.ml.feature import IDF
  idfizer = IDF(inputCol='tf_features', outputCol='tf_idf_features')
  idf_model = idfizer.fit(tf_result)
  tfidf_result = idf_model.transform(tf_result)
  from pyspark.ml.clustering import LDA

  num_topics = 3
  max_iter = 10

  lda = LDA(k=num_topics, maxIter=max_iter, featuresCol='tf_idf_features')
  lda_model = lda.fit(tfidf_result)
  from pyspark.sql import types as T
  vocab = tf_model.vocabulary
  def get_words(token_list):
      return [vocab[token_id] for token_id in token_list]
  udf_to_words = F.udf(get_words, T.ArrayType(T.StringType()))

  num_top_words = 15
  topics = lda_model.describeTopics(num_top_words).withColumn('topicWords', udf_to_words(F.col('termIndices')))
  topics_p=topics.toPandas()
  return topics_p

from pyspark.sql.functions import udf

def pd_2_spark(df_pd):
    sparkdf = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

    customSchema = StructType([
        StructField("id", StringType(), True), StructField("created_at", StringType(), False),
        StructField("date", StringType(), False)
        , StructField("tweet", StringType(), False), StructField("user_id", StringType(), False),
        StructField("user_id_str", StringType(), False)
        , StructField("username", StringType(), False), StructField("nlikes", StringType(), False),
        StructField("nretweets", StringType(), False)
    ])

    df = sparkdf.createDataFrame(df_pd, schema=customSchema)

    clean_tweets = df.cols.remove_accents("tweet") \
        .cols.remove_special_chars("tweet")

    tweets = clean_tweets.select("tweet").rdd.flatMap(lambda x: x).collect()


