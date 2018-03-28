import json
import csv

with open('WorkersData.json') as df:
    data = json.load(df)
for key, val in data.items():
    title1 = val['job_profile'][0]['title1']
    title2 = val['job_profile'][0]['title2']
    val.pop('job_profile', None)
    val['job_profile.title1'] = title1
    val['job_profile.title2'] = title2
with open('WorkersData.csv', 'w', newline='') as csvfile:
    flds = ['id'] + list(data['14e1c7e2-bc9c-11e7-96ec-34f39a5ff2b1'].keys())
    writer = csv.DictWriter(csvfile, fieldnames=flds, delimiter='\t')
    writer.writeheader()
    for key, val in data.items():
        tempDict = dict({'id': key})
        tempDict.update(val)
        writer.writerow(tempDict)
