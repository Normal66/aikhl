import requests
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import json
from bs4 import BeautifulSoup
from scipy.stats import poisson
import re
import config
import pandas as pd
import numpy as np
import collections

get_href = re.compile(r'href=\"([^\"]*)\"')

def obrabotka_protokol(i_url):
    # Парсит страницу и возвращает словарь - инфо о матче
    l_res = {}
    try:
        _r = requests.get(i_url)
        _src = BeautifulSoup(_r.text, 'lxml')
        _ligue, _command, _date, _season = _src.title.text.split(',')
        _match_count = _src.find('div', {'class': 'match-count'}).text
        _match_status = _src.find('div', {'class': 'match-status'}).text
#        l_res['ligue'] = _ligue.strip()
        l_res['season'] = _date.strip().split('-')[0]
        l_res['home'] = _command.strip().split('-')[0]
        l_res['away'] = _command.strip().split('-')[1]
#        l_res['season'] = _season.strip()
        g_home = _match_count.strip().split(':')[0]
        g_guest = _match_count.strip().split(':')[1]
        l_res['home_goals'] = g_home
        l_res['away_goals'] = g_guest
#        l_res['status'] = _match_status.strip()
    except:
        pass
    return l_res


def main(l_url):
    # Запускает потоки для парсинга результатов
    _res = []
    _i = 0
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for url in l_url:
            futures.append(executor.submit(obrabotka_protokol, i_url=url))
        for future in concurrent.futures.as_completed(futures):
            _res.append(future.result())
            _i += 1
            print(_i, ' из ', len(l_url), end='\r')
    with open('result.json', 'w', encoding='utf-8') as f:
        json.dump(_res, f, ensure_ascii=False, indent=4)
# ----------------------------------------------------------------------------------------------- #

def get_regular():
    # Возвращает список url результатов для игр регулярки    
    l_res = []
    for _url in config.list_regular_url:
        print('Get regular for: ', _url)
        r = requests.get(_url)
        _src = BeautifulSoup(r.text, 'html.parser')
        l_table = _src.findAll('div', {"class": "scroll-table-wrap"})
        for _items in l_table:
            table = _items.find('table', {"class": "tbl-stat"})
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                l_data = row.findAll('td', {'class': 'chess-data'})
                for _items_links in l_data:
                    _links = get_href.findall(str(_items_links))
                    if _links:
                        for _links_items in _links:
                            l_res.append('https://allhockey.ru' + _links_items)
    return l_res


def get_playoff():
    # Возвращает список url результатов для игр плейофф
    l_res = []
    for l_url in config.list_playoff_url:
        print('Get playoff for: ', l_url)
        r = requests.get(l_url)
        _src = BeautifulSoup(r.text, 'html.parser')
        l_table = _src.findAll('div', {"class": "playoff-series"})
        for _i in l_table:
            for i in _i.find_all('a'):
                _tmp_str = str(i.get('href'))
                l_res.append('https://allhockey.ru' + _tmp_str)
    return l_res


if __name__ == '__main__':
    _one = get_regular()
    print(len(_one))
    _two = get_playoff()
    print(len(_two))
    _obr = [*_one, *_two]
    print(len(_obr))
    main(_obr)
