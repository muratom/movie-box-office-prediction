import json
import csv

with open('./unprocessed_data/trailers.json', 'r') as file:
    metadata = json.load(file)

    data = []

    for i in range(1, 12001):
        k = str(i)
        m = metadata["trailers12k"][k]
        data.append({
            "imdb_id": 'tt' + m["imdb"]["id"],
            "title": m["imdb"]["title"],
            "year": m["imdb"]["year"],
            "trailer": 'https://youtu.be/' + m["youtube"]["trailers"][0]["id"]
        })
        print("process %s entry" % k)

    with open('./trailers.csv', 'w', newline='') as csvfile:
        fieldnames = ['imdb_id', 'trailer', 'title', 'year']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
