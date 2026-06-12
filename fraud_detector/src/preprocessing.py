import logging
import os

import numpy as np
import pandas as pd
from geopy.distance import great_circle
from sklearn.impute import SimpleImputer

logger = logging.getLogger(__name__)

TARGET_COL = 'target'
CATEGORICAL_COLS = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs']
DROP_COLS = ['name_1', 'name_2', 'street', 'post_code']
CONTINUOUS_COLS = ['amount', 'population_city']
N_CATS = 50

_cat_mappings = {}
_mean_enc_mappings = {}
_imputer = None
_all_continuous = None
_categorical_encoded = None
_time_cols = ['hour', 'year', 'month', 'day_of_month', 'day_of_week']

TRAIN_PATH = os.path.join(os.path.dirname(__file__), '..', 'train_data', 'train.csv')


def _add_time_features(df):
    df = df.copy()
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    dt = df['transaction_time'].dt
    df['hour'] = dt.hour
    df['year'] = dt.year
    df['month'] = dt.month
    df['day_of_month'] = dt.day
    df['day_of_week'] = dt.dayofweek
    df.drop(columns='transaction_time', inplace=True)
    return df


def _add_distance_features(df):
    df = df.copy()
    df['distance'] = df.apply(
        lambda x: great_circle(
            (x['lat'], x['lon']),
            (x['merchant_lat'], x['merchant_lon'])
        ).km,
        axis=1
    )
    return df.drop(columns=['lat', 'lon', 'merchant_lat', 'merchant_lon'])


def _precompute_mappings():
    global _cat_mappings, _mean_enc_mappings, _imputer, _all_continuous, _categorical_encoded

    logger.info('Loading training data from %s ...', TRAIN_PATH)
    train = pd.read_csv(TRAIN_PATH).drop(columns=DROP_COLS)
    logger.info('Raw train data imported. Shape: %s', train.shape)

    train = _add_time_features(train)

    for col in CATEGORICAL_COLS:
        new_col = col + '_cat'
        temp_df = train \
            .groupby(col, dropna=False)[[TARGET_COL]] \
            .count() \
            .sort_values(TARGET_COL, ascending=False) \
            .reset_index() \
            .set_axis([col, 'count'], axis=1) \
            .reset_index()
        temp_df['index'] = temp_df.apply(lambda x: np.nan if pd.isna(x[col]) else x['index'], axis=1)
        temp_df[new_col] = [
            'cat_NAN' if pd.isna(x) else 'cat_' + str(x) if x < N_CATS else f'cat_{N_CATS}+'
            for x in temp_df['index']
        ]
        train = train.merge(temp_df[[col, new_col]], how='left', on=col)
        mapping_df = train[[col, new_col]].drop_duplicates()
        _cat_mappings[col] = dict(zip(mapping_df[col], mapping_df[new_col]))

    train = _add_distance_features(train)

    _categorical_encoded = [c + '_cat' for c in CATEGORICAL_COLS] + _time_cols

    for col in _categorical_encoded:
        mean_series = train.groupby(col)[[TARGET_COL]].mean()[TARGET_COL]
        _mean_enc_mappings[col] = mean_series.to_dict()

    _all_continuous = CONTINUOUS_COLS + ['distance']

    imputer = SimpleImputer(missing_values=np.nan, strategy='mean')
    _imputer = imputer.fit(train[_all_continuous])

    logger.info('Mappings precomputed. Cat cols: %d, Mean-enc cols: %d', len(_cat_mappings), len(_mean_enc_mappings))


def run_preproc(input_df):
    global _cat_mappings, _mean_enc_mappings, _imputer, _all_continuous, _categorical_encoded

    if not _cat_mappings:
        _precompute_mappings()

    df = input_df.copy()
    df = df.drop(columns=[c for c in DROP_COLS if c in df.columns])

    for col in CATEGORICAL_COLS:
        new_col = col + '_cat'
        mapping = _cat_mappings[col]
        df[new_col] = df[col].map(mapping).fillna('cat_NAN')
        df.drop(columns=col, inplace=True)

    df = _add_time_features(df)

    for col in _categorical_encoded:
        df[col] = df[col].fillna('cat_NAN')
        mean_dict = _mean_enc_mappings[col]
        mean_col = f'{col}_mean_enc'
        df[mean_col] = df[col].map(mean_dict)

    df = _add_distance_features(df)

    cont_imputed = _imputer.transform(df[_all_continuous])
    remaining_cols = [c for c in df.columns if c not in _all_continuous + _categorical_encoded]
    out = pd.concat([df[remaining_cols].reset_index(drop=True),
                     pd.DataFrame(cont_imputed, columns=_all_continuous)], axis=1)

    for col in _all_continuous:
        out[col + '_log'] = np.log(out[col] + 1)
        out.drop(columns=col, inplace=True)

    drop_cols = [TARGET_COL] if TARGET_COL in out.columns else []
    out = out.drop(columns=drop_cols)

    logger.info('Preprocessing complete. Output shape: %s', out.shape)
    return out
