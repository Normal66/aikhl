import json
import os
import pickle
from sklearn.experimental import enable_halving_search_cv  # noqa
import pandas as pd

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'


def do_clean_data():
    with open('result.json', 'r', encoding='utf-8') as fp:
        data = json.load(fp)
    _src = pd.json_normalize(data)
    _src['season'] = _src['season'].map(lambda s: s.split('.')[2])
    _src = _src.loc[_src['home_goals'] != '–']
    _src = _src.loc[_src['home_goals'] != '-']
    _src = _src.loc[_src['away_goals'] != '–']
    _src = _src.loc[_src['away_goals'] != '-']

    _src = _src.sort_values(by='season')
    _src.home_goals = _src.home_goals.str.strip().astype(int)
    _src.away_goals = _src.away_goals.str.strip().astype(int)

    _new = _src.copy()
    _new['winner'] = ''
    _new['looser'] = ''

    _new.loc[(_new.home_goals > _new.away_goals), 'winner'] = _new.home
    _new.loc[(_new.home_goals > _new.away_goals), 'looser'] = _new.away

    _new.loc[(_new.home_goals < _new.away_goals), 'winner'] = _new.away
    _new.loc[(_new.home_goals < _new.away_goals), 'looser'] = _new.home
    _new = _new.drop_duplicates()
    return _new
# ----------------------------------------------------------------------------------------------------- #
def GetSeasonTeamStat(_df, _team, _season):
    _src = _df.loc[_df['season'] == str(_season)]
    _home = _src.loc[_src['home'] == _team]
    _away = _src.loc[_src['away'] == _team]
    _home = _home.drop_duplicates()
    _away = _away.drop_duplicates()
    # Всего матчей
    _all_match = len(_home) + len(_away)
    # Шайб забито дома
    _home_goals_winner = _home.home_goals.sum()
    # Шайб пропущено дома
    _home_goals_looser = _home.away_goals.sum()
    # Шайб забито в гостях
    _away_goals_winner = _away.home_goals.sum()
    # Шайб пропущено дома
    _away_goals_looser = _away.away_goals.sum()
    # Всего шайб забито
    _all_goals_win = _home_goals_winner + _away_goals_winner
    # Всего шайб пропущено
    _all_goals_los = _home_goals_looser + _away_goals_looser
    # Всего очков
    _totalScore = 0
    # Побед и поражений дома
    _all_home_win = 0
    _all_home_los = 0
    for i, row in _home.iterrows():
        if row['home_goals'] > row['away_goals']:
            _all_home_win += 1
            _totalScore += 3
        else:
            _all_home_los += 1
            _totalScore += 1
    # Побед и поражений в гостях
    _all_away_win = 0
    _all_away_los = 0
    for i, row in _away.iterrows():
        if row['away_goals'] > row['home_goals']:
            _all_away_win += 1
            _totalScore += 3
        else:
            _all_away_los += 1
            _totalScore += 1
    _all_win = _all_home_win + _all_away_win
    _all_los = _all_home_los + _all_away_los
    return [_all_match, _totalScore, _home_goals_winner, _home_goals_looser, _away_goals_winner, _away_goals_looser, _all_goals_win, _all_goals_los]
# ---------------------------------------------------------------------------------------------------------- #
def createGamePrediction(_model, team1_vector, team2_vector):
    diff = [[a - b for a, b in zip(team1_vector, team2_vector)]]
    predictions = _model.predict(diff)
    return predictions

if __name__ == '__main__':
    # Загружаем
    _src = do_clean_data()
    # Загружаем
    pkl_filename = "khl_model.pkl"
    with open(pkl_filename, 'rb') as file:
        _model = pickle.load(file)

    print('Собственно предсказываем :)')

    team1_name = "Сибирь"
    team2_name = 'Трактор'
    team1_vector = GetSeasonTeamStat(_src, team1_name, 2021)
    team2_vector = GetSeasonTeamStat(_src, team2_name, 2021)
    print(team1_name, createGamePrediction(_model, team1_vector, team2_vector), " - ", team2_name,
          createGamePrediction(_model, team2_vector, team1_vector, ))
