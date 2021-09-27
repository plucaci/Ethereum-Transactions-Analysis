import json as jsn

scams = {}

def filter():
    with open("scams.json") as f:
        scams_data = jsn.load(f)

        for k, v in scams_data['result'].items():

            id = str(v['id'])
            category = v['category'].lower()
            status = v['status'].lower()

            scams[id] = {'category': category, 'status': status}
filter()

for k, v in scams.items():
    print(f"{k},{scams[k]['category']},{scams[k]['status']}")