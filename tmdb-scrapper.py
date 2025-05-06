import pandas
import requests
from bs4 import BeautifulSoup

def find_budget(id):
    url = 'https://api.themoviedb.org/3/movie/{0}?api_key=e531ea4e8726029dc989d2fae8f0bf3c'
    needed_headers = {
        'Host': 'api.themoviedb.org',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Cookie': 'OptanonConsent=isGpcEnabled=0&datestamp=Wed+May+15+2024+01%3A26%3A40+GMT%2B0300+(Moscow+Standard+Time)&version=202310.2.0&hosts=&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'DNT': '1',
        'Sec-GPC': '1',
        'If-None-Match': 'W/"e9d9c742a96ecdb1a23d1ae32084aa76"',
    }
    resp = requests.get(url.format(id))

    return resp

df = pandas.read_csv('./missing_budget.csv')

tmdb_ids = df['tmdb_id'].to_list()

print(find_budget(15602))

