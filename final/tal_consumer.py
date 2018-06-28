import json
import re
from datetime import datetime

from kafka import KafkaConsumer

SCHEMA_DEF = ['id', 'stop_date', 'location_raw', 'driver_gender',
              'driver_race', 'violation', 'search_conducted', 'is_arrested']
LEFT_SCHEMA = [fld for fld in SCHEMA_DEF if fld not in ['is_arrested', 'violation', 'location_raw', 'driver_gender']]


def msg_cleaner(msg):
    result = re.sub("[()']", '', msg.value.decode('utf8')).split(",")
    return [word.strip() for word in result]


def str_to_bool(word):
    return word.lower() == "true"


def data_filter(msg_dict):  # returns true if data is ok
    return msg_dict['location_raw'].title() in ['San Diego', 'Redding', 'Buttonwillow', 'Modesto'] and \
           msg_dict['search_conducted'].lower() in ['true', 'false'] and \
           msg_dict['is_arrested'].lower() in ['true', 'false'] and \
           msg_dict['driver_gender'].lower() in ['m', 'f'] and \
           re.match('(\d{4})[/.-](\d{2})[/.-](\d{2})$', msg_dict['stop_date'])


def data_convert(msg_dict):
    msg_dict['search_conducted'] = str_to_bool(msg_dict['search_conducted'])
    msg_dict['is_arrested'] = str_to_bool(msg_dict['is_arrested'])
    msg_dict['stop_date'] = datetime.strptime(msg_dict['stop_date'], "%Y-%m-%d")
    msg_dict['driver_gender'] = msg_dict['driver_gender'].upper()
    msg_dict['location_raw'] = msg_dict['location_raw'].title()


# distribute data data into indices in elasticsearch
# using our distribution method from H.W.1
def tal_distributer(es, i, msg_dict):
    # black panther - redding:
    if msg_dict['search_conducted'] and \
            msg_dict['stop_date'] >= datetime(2016, 1, 30):
        bp_json = json.dumps(msg_dict, default=str)
        es.index(index='reddings', doc_type='json', id=i, body=bp_json)

    else:
        # spiderpig - buttonwillow
        loc_json = json.dumps({"location_raw": msg_dict["location_raw"]}, default=str)
        es.index(index='buttonwillow_loc', doc_type='json', id=i, body=loc_json)

        # spiderman all genders
        gender_json = json.dumps({"driver_gender": msg_dict["driver_gender"]}, default=str)
        es.index(index='modesto_gender', doc_type='json', id=i, body=gender_json)

        # superman - San Diego
        viol_json = json.dumps({"violation": msg_dict["violation"]}, default=str)

        if msg_dict['driver_gender'] == 'F':  # all females from bp_left
            es.index(index='sandiego_viol', doc_type='json', id=i, body=viol_json)

        else:
            if msg_dict['location_raw'] == 'Modesto':
                es.index(index='modesto_male_viol', doc_type='json', id=i, body=viol_json)
            elif msg_dict['location_raw'] == 'San Diego':
                es.index(index='sandiego_male_viol', doc_type='json', id=i, body=viol_json)
            elif msg_dict['location_raw'] == 'Redding':
                es.index(index='redding_male_viol', doc_type='json', id=i, body=viol_json)
            elif msg_dict['location_raw'] == 'Buttonwillow':
                es.index(index='buttonwillow_male_viol', doc_type='json', id=i, body=viol_json)

        arstd_json = json.dumps({"is_arrested": msg_dict["is_arrested"]}, default=str)
        # NevadaArrested
        if msg_dict['location_raw'] != 'Modesto' and msg_dict['driver_gender'] == 'F':
            es.index(index='sandiego_arrested', doc_type='json', id=i, body=arstd_json)

        # spiderman - Modesto (CaliforniaArrested)
        else:
            es.index(index='modesto_arrested', doc_type='json', id=i, body=arstd_json)

        left_json = json.dumps({k: msg_dict[k] for k in LEFT_SCHEMA}, default=str)
        es.index(index='leftovers', doc_type='json', id=i, body=left_json)


# set connection to kafka (by given ip, port & topic)
# returns a generator of messages and schema (assuming 1st line is schema)
def tal_consumer(ip, port, topic):
    consumer = KafkaConsumer(topic,
                             bootstrap_servers='{}:{}'.format(ip, port),
                             auto_offset_reset='earliest')

    first = next(consumer)
    schema = msg_cleaner(first)

    return consumer, schema


def main():
    consumer, schema = tal_consumer("104.209.178.73", "5601", "FPA")

    es = Elasticsearch()

    passed_filter = 1
    for i, msg in enumerate(consumer, 1):

        print("message number:", i)

        val_ls = msg_cleaner(msg)

        # keeps only wanted data
        msg_dict = dict([(k, v) for k, v in zip(schema, val_ls) if k in SCHEMA_DEF])

        if data_filter(msg_dict) == False:
            continue

        data_convert(msg_dict)
        print("Passed filters count", passed_filter)
        passed_filter += 1

        # go to elastic.......
        tal_distributer(es, i, msg_dict)

    return


if __name__ == '__main__':
    main()

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import Row

from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql.functions import unix_timestamp, to_date, date_format

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.stat import Statistics


# creates a dictionary of index name : index DataFrame
# note: creates it for all indices in elasticsearch instance.
def create_indices_dict(es):  # returns array of indices
    array = {}
    for index in es.indices.get_alias("*").keys():
        index = str(index)
        res = es.search(index=index, size=90000)
        items = map(lambda item: {k: v for k, v in item['_source'].items() + [('_id', int(item['_id']))]},
                    res['hits']['hits'])
        rows = map(lambda item: Row(**item), items)
        array[index] = sqlContext.createDataFrame(rows)
    return array


# uses the indices dictionary to build the original dataset
# assuming distribution that we used in H.W 1.
def get_combined_df(indices_dict):  # input is dict of {indexName:indexDF}
    viol_array = {k: v for k, v in indices_dict.items() if
                  k in ['buttonwillow_male_viol', 'modesto_male_viol', 'redding_male_viol', 'sandiego_male_viol']}
    viol_union = indices_dict['sandiego_viol']
    for index, df in viol_array.items():
        viol_union = viol_union.union(df)

    arstd_union = indices_dict['modesto_arrested'].union(indices_dict['sandiego_arrested'])
    unioned_joined = viol_union.join(arstd_union, ['_id'])

    loc_joined = unioned_joined.join(indices_dict['buttonwillow_loc'], ['_id'])
    gender_joined = loc_joined.join(indices_dict['modesto_gender'], ['_id'])
    left_joined = gender_joined.join(indices_dict['leftovers'], ['_id'])

    left_joined = left_joined.select(
        ['_id', 'id', 'stop_date', 'location_raw', 'driver_gender', 'driver_race', 'violation', 'search_conducted',
         'is_arrested'])
    indices_dict['reddings'] = indices_dict['reddings'].select(
        ['_id', 'id', 'stop_date', 'location_raw', 'driver_gender', 'driver_race', 'violation', 'search_conducted',
         'is_arrested'])

    combined_data = left_joined.union(indices_dict['reddings'])
    return combined_data


# input is combined data, creates from it features and labels for classifications.
# returns df with features,labels cols.
def create_features_labels(combined_data):
    combined_data = combined_data.withColumn('date', (combined_data.stop_date).cast("date"))
    combined_data = combined_data.withColumn('week_end', (date_format(combined_data.date, 'u') > 4).cast('integer'))

    indexer = StringIndexer(inputCol="location_raw", outputCol="location_raw_index")
    combined_data = indexer.fit(combined_data).transform(combined_data)

    indexer = StringIndexer(inputCol="driver_gender", outputCol="driver_gender_index")
    combined_data = indexer.fit(combined_data).transform(combined_data)

    indexer = StringIndexer(inputCol="driver_race", outputCol="driver_race_index")
    combined_data = indexer.fit(combined_data).transform(combined_data)

    indexer = StringIndexer(inputCol="violation", outputCol="violation_index")
    combined_data = indexer.fit(combined_data).transform(combined_data)

    combined_data = combined_data.withColumn('search_conducted_binary',
                                             (combined_data.search_conducted).cast('integer'))
    combined_data = combined_data.withColumn('label', (combined_data.is_arrested).cast('integer'))

    assembler = VectorAssembler(
        inputCols=["location_raw_index", "driver_gender_index", "driver_race_index", "violation_index",
                   "search_conducted_binary", "week_end"], outputCol="features")
    combined_data = assembler.transform(combined_data)

    to_learn_data = combined_data.select("features", "label")
    return to_learn_data


# creates predictions for structed df (features, labels) using RandomForest model.
# returns a dataframe with the predictions.
def get_predictions_df(data, tree_num=10):
    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

    # Automatically identify categorical features, and index them.
    # Set maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=10).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.8, 0.2])

    # Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=tree_num)

    # Convert indexed labels back to original labels.
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)

    # Chain indexers and forest in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)
    return predictions


# evaluates our predictions using standard measurements(percision, accuracy, recall & F1)
# returns the measurements.
def evaluation(prd_df):
    prd_df = prd_df.withColumn('TP', ((prd_df.label == 1) & (prd_df.predictedLabel == 1)).cast('integer'))
    prd_df = prd_df.withColumn('TN', ((prd_df.label == 0) & (prd_df.predictedLabel == 0)).cast('integer'))
    prd_df = prd_df.withColumn('FN', ((prd_df.label == 1) & (prd_df.predictedLabel == 0)).cast('integer'))
    prd_df = prd_df.withColumn('FP', ((prd_df.label == 0) & (prd_df.predictedLabel == 1)).cast('integer'))
    tp = sum(x.TP for x in prd_df.select('TP').collect())
    tn = sum(x.TN for x in prd_df.select('TN').collect())
    fn = sum(x.FN for x in prd_df.select('FN').collect())
    fp = sum(x.FP for x in prd_df.select('FP').collect())
    accuracy = float(tp + tn) / (tp + tn + fp + fn)
    percision = float(tp) / (tp + fp)
    recall = float(tp) / (tp + fn)
    f1measure = 2 * (recall * percision) / (recall + percision)
    return {"Accuracy": accuracy, "Percision": percision, "Recall": recall, "F1 Score": f1measure}


# Check correlation between violation type and is_arrested, can be splitted by gender.
def violation_correl(data, gender=None):  # checks correlations by gender
    if gender:
        cor_data = data.filter(data.driver_gender == gender)
    else:
        cor_data = data
    cor_data = cor_data.withColumn('binary_arrest', (cor_data.is_arrested).cast('integer'))
    cor_data = cor_data.withColumn('binary_EQ', (cor_data.violation == 'Equipment').cast('integer'))
    cor_data = cor_data.withColumn('binary_DUI', (cor_data.violation == 'DUI').cast('integer'))
    cor_data = cor_data.withColumn('binary_Other', (cor_data.violation == 'Other').cast('integer'))
    cor_data = cor_data.withColumn('binary_MV', (cor_data.violation == 'Moving violation').cast('integer'))

    arrest = [x.binary_arrest for x in cor_data.select('binary_arrest').collect()]
    seriesA = sc.parallelize(arrest)
    EQ = [x.binary_EQ for x in cor_data.select('binary_EQ').collect()]
    DUI = [x.binary_DUI for x in cor_data.select('binary_DUI').collect()]
    Other = [x.binary_Other for x in cor_data.select('binary_Other').collect()]
    MV = [x.binary_MV for x in cor_data.select('binary_MV').collect()]

    viols = {'Equipment violation': EQ,
             'DUI violation': DUI,
             'Other violation': Other,
             'Moving violation': MV
             }

    for viol, lis in viols.items():
        tempSeries = sc.parallelize(lis)
        viol_correl = Statistics.corr(seriesA, tempSeries, method="pearson")
        print "The Correlation between {} to being arrested is: {}".format(viol, viol_correl)


def race_correl(data):
    cor_data = data.withColumn('binary_arrest', (data.is_arrested).cast('integer'))
    arrest = [x.binary_arrest for x in cor_data.select('binary_arrest').collect()]
    seriesA = sc.parallelize(arrest)
    races = set([str(x[0]) for x in data.select('driver_race').collect()])
    for race in races:
        cor_data = cor_data.withColumn(race, (cor_data.driver_race == race).cast('integer'))

    racedict = {'White': [x.White for x in cor_data.select('White').collect()],
                'Asian': [x.Asian for x in cor_data.select('Asian').collect()],
                'Black': [x.Black for x in cor_data.select('Black').collect()],
                'Other': [x.Other for x in cor_data.select('Other').collect()],
                'Hispanic': [x.Hispanic for x in cor_data.select('Hispanic').collect()]}

    for race, lis in racedict.items():
        tempSeries = sc.parallelize(lis)
        race_correl = Statistics.corr(seriesA, tempSeries, method="pearson")
        print "The correlation between being {} to being arrested is: {}".format(race, race_correl)