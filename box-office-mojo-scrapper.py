import requests
from bs4 import BeautifulSoup

url = 'https://www.boxofficemojo.com/title/{0}'
req = requests.get(url.format('tt0250687'))
soup = BeautifulSoup(req.text)

domesitc_opening_elem = soup.find(string='Domestic Opening')
do = domesitc_opening_elem.parent.nextSibling.find(class_='money')
do = do.get_text()
do = do[1:]
do = int(do.replace(',', ''))
print(do)

mpaa_elem = soup.find(string='MPAA')
mpaa = mpaa_elem.parent.nextSibling
print(mpaa.get_text())

'''
TODO:
1) Скачать датасет
2) Отфильтровать его (убрать фильмы с пустыми "важными" полями)
3) С помощью df[imdb_id].to_list() достать ID фильмов
4) Веб-скрапинг с помощью скрипта выше
5) Добавление новых данных в датасет
'''

