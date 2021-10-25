import collections
import json
import os
import pickle
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.experimental import enable_halving_search_cv  # noqa
import pandas as pd

from sklearn.model_selection import train_test_split

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
    return [ _all_match, _totalScore, _home_goals_winner, _home_goals_looser, _away_goals_winner, _away_goals_looser, _all_goals_win, _all_goals_los]


def GetSeasonAllTeamStat(_df, season):
    teamList = _df['home'].tolist()
    annual = collections.defaultdict(list)
    for team in teamList:
        team_vector = GetSeasonTeamStat(_df, team, season)
        annual[team] = team_vector
    return annual

# ---------------------------------------------------------------------------------------------------------- #
def GetTrainingData(_df, seasons):
    totalNumGames = 0
    for season in seasons:
        annual = _df[_df['season'] == str(season)]
        totalNumGames += len(annual.index)

    numFeatures = len(GetSeasonTeamStat(_df, 'Автомобилист', 2016))  # случайная команда для определения размерности
    xTrain = np.zeros((totalNumGames, numFeatures))
    yTrain = np.zeros((totalNumGames))
    indexCounter = 0

    for season in seasons:
        team_vectors = GetSeasonAllTeamStat(_df, str(season))
        annual = _df[_df['season'] == str(season)]
        numGamesInYear = len(annual.index)
        xTrainAnnual = np.zeros((numGamesInYear, numFeatures))
        yTrainAnnual = np.zeros((numGamesInYear))
        counter = 0
        for index, row in annual.iterrows():
            team = row['home']
            t_vector = team_vectors[team]
            rivals = row['away']
            r_vector = team_vectors[rivals]

            diff = [a - b for a, b in zip(t_vector, r_vector)]

            if len(diff) != 0:
                xTrainAnnual[counter] = diff
            if team == row['winner']:
                yTrainAnnual[counter] = 1
            else:
                yTrainAnnual[counter] = 0
            counter += 1
        xTrain[indexCounter:numGamesInYear + indexCounter] = xTrainAnnual
        yTrain[indexCounter:numGamesInYear + indexCounter] = yTrainAnnual
        indexCounter += numGamesInYear
    return xTrain, yTrain


def prepare_and_training():
    print('Очистка данных')
    _src = do_clean_data()
    years = range(2015, 2022)


    print('Подготовка данных')
    xTrain, yTrain = GetTrainingData(_src, years)
    X_train, X_test, y_train, y_test = train_test_split(xTrain, yTrain, test_size=0.2, shuffle=False)
    print('Тренировка модели RFC')
    rs = RandomForestClassifier(bootstrap=True, ccp_alpha=0.0, class_weight=None,
                                criterion='gini', max_depth=7, max_features='log2',
                                max_leaf_nodes=None, max_samples=None,
                                min_impurity_decrease=0.0, min_samples_leaf=28, min_samples_split=7,
                                min_weight_fraction_leaf=0.0, n_estimators=400,
                                n_jobs=None, oob_score=False, random_state=None,
                                verbose=0, warm_start=False)
    rs.fit(X_train, y_train)
    print(rs.score(X_train, y_train))

    # Сохраняем модель
    pkl_filename = "khl_model.pkl"
    with open(pkl_filename, 'wb') as file:
        pickle.dump(rs, file)
    return


if __name__ == '__main__':
    # Обучаем
    prepare_and_training()
