import requests
import csv
from pytubefix import YouTube
from pytubefix.cli import on_progress
from pytubefix.exceptions import AgeRestrictedError 
from pathlib import Path
from time import sleep

counter = 0
not_downloaded = []
start_pos = 0

with open('./trailers_to_download.csv') as f:
    reader = csv.reader(f)
    next(f)

    for row in reader:
        id = row[0]
        url = row[1]

        t = Path(f'./trailers/{id}.mp4')
        if t.is_file():
            print(f"Trailer for {id} is already downloaded")
        else:
            try:
                yt = YouTube(url, client='WEB', on_progress_callback=on_progress, use_po_token=True, token_file='po_token.json')
                # yt = YouTube(url, use_oauth=True, allow_oauth_cache=True, on_progress_callback=on_progress)
                print(f"Downloading trailer for {id} by link {url}: {yt.title}")
                ys = yt.streams.get_lowest_resolution(progressive=False)
                ys.download(output_path='./trailers', filename=f'{id}.mp4')
                sleep(2.0)
            except Exception as e:
                print(f'Exception for {id} and {url}: {e}')
                not_downloaded.append(id)
            
        counter += 1
        print(counter)

print("counter:", counter)
print("not downloaded:", not_downloaded)
