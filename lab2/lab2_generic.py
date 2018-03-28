import json
import csv


def json_walk(node, prefix=""):
    for key, item in node.items():
        if isinstance(item, list):
            prefix += key + '.'
            for listIter in item:
                for temp in json_walk(listIter, prefix):
                    yield temp
        else:
            yield {prefix + key: item}


def json_to_csv(filename):
    with open(filename) as df:
        data = json.load(df)
    with open('Generic.csv', 'w', newline='') as csvfile:
        cnt = 1
        for key, val in data.items():
            tempDict = dict({'id': key})
            for dictTuple in json_walk(val):
                tempDict.update(dictTuple)
            if (cnt == 1):
                flds = list(tempDict.keys())
                writer = csv.DictWriter(csvfile, fieldnames=flds, delimiter='\t')
                writer.writeheader()
            cnt += 1
            writer.writerow(tempDict)
