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
    # {'ligue': 'КХЛ', 'command': 'Автомобилист-Металлург Мг', 'date_game': '29.08.2016',
    # 'season': '2016/2017 Регулярный чемпионат', 'goals': '2:0', 'status' : 'Матч завершён'}
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


def zzz():
    l_res = []
    l_url = ' https://allhockey.ru/stat/khl/2020/116/table'
    print('Get playoff for: ', l_url)
    r = requests.get(l_url)
    _src = BeautifulSoup(r.text, 'html.parser')
    l_table = _src.findAll('div', {"class": "playoff-series"})
    for _i in l_table:
        for i in _i.find_all('a'):
            _tmp_str = str(i.get('href'))
            l_res.append('https://allhockey.ru' + _tmp_str)
    print(len(l_res))

def main(l_url):
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
    # Чистим данные
#    _dst = pd.json_normalize(_res)
#    _dst['season'] = pd.to_datetime(_dst['season'])
#    _out_df = _dst.sort_values(by='date_game')
#    _out_df.to_csv('results.csv')
    with open('result.json', 'w', encoding='utf-8') as f:
        json.dump(_res, f, ensure_ascii=False, indent=4)


def get_regular():
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


def do_clear():
    # Очистка данных
    with open('result.json', 'r', encoding='utf-8') as fp:
        data = json.load(fp)
    _src = pd.json_normalize(data)
    _src['date_game'] = pd.to_datetime(_src['date_game'])
    _out_df = _src.sort_values(by='date_game')
    _out_df = _out_df.drop_duplicates()
    _out_df['status'] = _out_df['status'].map(lambda s: s.replace('\n', ' ').replace('  ', '').strip())
    _out_df.loc[(_out_df['status'] == 'Матч завершён'), 'status'] = 'M'
    _out_df.loc[(_out_df['status'] == 'Матч завершён по буллитам'), 'status'] = 'B'
    _out_df.loc[(_out_df['status'] == 'Матч завершёнв овертайме'), 'status'] = 'O'
    _out_df.loc[(_out_df['status'] == 'Матч не начался'), 'status'] = 'N'
    _out_df = _out_df.loc[_out_df['status'] != 'N']
    _out_df = _out_df.loc[_out_df['goals_home'] != '-']
    _out_df = _out_df.loc[_out_df['goals_home'] != '+']
    _out_df = _out_df.loc[_out_df['goals_guest'] != '-']
    _out_df = _out_df.loc[_out_df['goals_guest'] != '+']
    _out_df.to_csv('ai.csv', encoding='utf-8', index=False)

    _out_df = _out_df.drop('date_game', 1)
    _out_df = _out_df.drop('W1', 1)
    _out_df = _out_df.drop('W2', 1)
    _out_df = _out_df.drop('status', 1)
    _out_df.goals_home = _out_df.goals_home.str.strip().astype(int)
    _out_df.goals_guest = _out_df.goals_guest.str.strip().astype(int)

    # Calculate number of games played
    n_games = int(len(_out_df) / len(_out_df.home.unique()))

    games = _out_df.iloc[:, 0:]

    # Create a new df with stats per team
    # Home stats
    shl_stats_home = _out_df.groupby(["home"]).sum()
    shl_stats_home.reset_index(level=0, inplace=True)
    shl_stats_home.rename(columns={"home": "team", "goals_guest": "home_ga"}, inplace=True)

    max_home = _out_df.groupby(["home"]).max()
    max_home = max_home.filter(items=["home", "goals_home", "goals_guest"])
    max_home.rename(columns={"goals_home": "home_max", "goals_guest": "home_ga_max"}, inplace=True)
    max_home.reset_index(level=0, inplace=True)
    # Away stats
    shl_stats_away = _out_df.groupby(["guest"]).sum()
    shl_stats_away.reset_index(level=0, inplace=True)
    shl_stats_away.rename(columns={"goals_home": "away_ga"}, inplace=True)
    max_away = _out_df.groupby(["guest"]).max()
    max_away = max_away.filter(items=["guest", "goals_home", "goals_guest"])
    max_away.rename(columns={"goals_home": "away_ga_max", "goals_guest": "away_max"}, inplace=True)
    max_away.reset_index(level=0, inplace=True)
    # concat
    shl_stats = pd.concat([shl_stats_home, shl_stats_away, max_home, max_away], axis=1)
    shl_stats.drop(["guest", "home"], inplace=True, axis=1)
    shl_stats = shl_stats.loc[:, ~shl_stats.columns.str.contains('^Unnamed')]
    # Average for each stat
    shl_stats["home_avg"] = shl_stats.goals_home / (n_games)
    shl_stats["away_avg"] = shl_stats.goals_guest / (n_games)
    shl_stats["home_ga_avg"] = shl_stats.home_ga / (n_games)
    shl_stats["away_ga_avg"] = shl_stats.away_ga / (n_games)

    shl_stats.to_csv("shl_clean.csv", encoding='utf-8')
    games.to_csv("games.csv", encoding='utf-8')


# What we need:
# Poisson calculation where the team and goal max is dynamic rather hand hard coded
def pois (df, team, max_g, avg_g):
    avg = df.loc[team, avg_g]
    ma = df.loc[team, max_g]
    pois = [poisson.pmf(i, avg) for i in range(ma+1)]
    return(pois)


def sim_game(_df, home_team, away_team):
    home = pois(_df, home_team, "home_max", "home_avg")
    away = pois(_df, away_team, "away_max", "away_avg")
    matrix = np.multiply.outer(home, away)
    home_prob = np.sum(np.tril(matrix, -1))/np.sum(matrix)
    away_prob = np.sum(np.triu(matrix, 1))/np.sum(matrix)
    tie_prob = np.sum(np.diagonal(matrix))/np.sum(matrix)
    winner = max(home_prob, away_prob, tie_prob)
    print("The probability of {} winning is: {:.2%}".format(home_team, home_prob))
    print("The probability of {} winning is: {:.2%}".format(away_team, away_prob))
    print("The probability of a tie is: {:.2%}".format(tie_prob))



def test():
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    from scipy.stats import poisson
    import seaborn as sns
    import random, statistics

    df = pd.read_csv("shl_clean.csv")
    df = df.iloc[:, 1:]
    df.set_index("team", inplace=True)
    games = pd.read_csv("games.csv")
    games = games.iloc[:, 1:]
    schedule = games.iloc[:, :2]

    home = pois(df, "Автомобилист", "home_max", "home_avg")
    away = pois(df, "Трактор", "away_max", "away_avg")

    matrix = np.multiply.outer(home, away)
    np.set_printoptions(suppress=True)
    print(matrix)
    print(np.sum(matrix))
    res = sns.heatmap(matrix, annot=True)
    print(res)
    np.diagonal(matrix)
    np.triu(matrix, 1)
    print(np.sum(np.tril(matrix, -1) * 100))
    home_prob = np.sum(np.tril(matrix, -1)) / np.sum(matrix)
    away_prob = np.sum(np.triu(matrix, 1)) / np.sum(matrix)
    tie_prob = np.sum(np.diagonal(matrix)) / np.sum(matrix)
    res = random.randrange(1, 100)
    print('-------')
    print(home_prob)
    print(away_prob)
    print(tie_prob)
    print(res)
    print(np.sum((away_prob, tie_prob)) * 100)

    if res > np.sum((away_prob, tie_prob), dtype=np.int32) * 100:
        print("Home win!")
    elif res > tie_prob * 100:
        print("Away win!")
    else:
        print("Tie")

    sim_game(df, "Сибирь", "Трактор")


if __name__ == '__main__':
#    _one = list(set(get_regular()))
    _one = get_regular()
    print(len(_one))
    _two = get_playoff()
    print(len(_two))
    _obr = [*_one, *_two]
    print(len(_obr))
    main(_obr)
