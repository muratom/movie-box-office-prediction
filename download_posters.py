import requests
import csv

empty_counter = 0
network_errors = {}

with open('to_download_posters.csv') as f:
    reader = csv.reader(f)
    next(f)
    for row in reader:
        id = row[0]
        
        url = row[1]
        r = requests.get(url)

        if (len(id) == 0 or len(url) == 0):
            print('empty value for id or url')
            empty_counter += 1
            continue
        if (r.status_code // 100 != 2):
            print(f'({r.status_code}) failed to download poster for {id}, located on {url}')
            if (r.status_code not in network_errors.keys()):
                network_errors[r.status_code] = 0
            network_errors[r.status_code] += 1
            continue

        with open(f'./posters/{id}.jpg', 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)

print("empty:", empty_counter)
print("network errors:", network_errors)
