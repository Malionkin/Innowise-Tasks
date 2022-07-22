import pandas as pd
from datetime import datetime
from loguru import logger


def log_alert_10_errors_in_a_minute(data):
    data = data.rename(columns={
        '0': 'error_code',
        '1': 'error_message',
        '2': 'severity',
        '3': 'log_location',
        '4': 'mode',
        '5': 'model',
        '6': 'graphics',
        '7': 'session_id',
        '8': 'sdkv',
        '9': 'test_mode',
        '10': 'flow_id',
        '11': 'flow_type',
        '12': 'sdk_date',
        '13': 'publisher_id',
        '14': 'game_id',
        '15': 'bundle_id',
        '16': 'appv',
        '17': 'language',
        '18': 'os',
        '19': 'adv_id',
        '20': 'gdpr',
        '21': 'ccpa',
        '22': 'country_code',
        '23': 'date',
        })
    data.date = [datetime.fromtimestamp(x) for x in data.date]
    error_data = data[data.severity == 'Error']
    json_log_1 = error_data.groupby([error_data.date.map(lambda t: \
                                    t.month),
                                    error_data.date.map(lambda t: \
                                    t.day),
                                    error_data.date.map(lambda t: \
                                    t.hour),
                                    error_data.date.map(lambda t: \
                                    t.minute)]).severity.value_counts().loc[lambda x: \
            x > 10].to_frame().to_json(orient='index')
    print(json_log_1)


def log_alert_for_bundle_id(data):
    data = data.rename(columns={
        '0': 'error_code',
        '1': 'error_message',
        '2': 'severity',
        '3': 'log_location',
        '4': 'mode',
        '5': 'model',
        '6': 'graphics',
        '7': 'session_id',
        '8': 'sdkv',
        '9': 'test_mode',
        '10': 'flow_id',
        '11': 'flow_type',
        '12': 'sdk_date',
        '13': 'publisher_id',
        '14': 'game_id',
        '15': 'bundle_id',
        '16': 'appv',
        '17': 'language',
        '18': 'os',
        '19': 'adv_id',
        '20': 'gdpr',
        '21': 'ccpa',
        '22': 'country_code',
        '23': 'date',
        })
    data.date = [datetime.fromtimestamp(x) for x in data.date]
    error_data = data[data.severity == 'Error']
    json_log_2 = error_data.groupby([error_data.bundle_id,
                                    error_data.date.map(lambda t: \
                                    t.month),
                                    error_data.date.map(lambda t: \
                                    t.day),
                                    error_data.date.map(lambda t: \
                                    t.hour)]).severity.value_counts().loc[lambda s: \
            s > 10].to_frame().to_json(orient='index')
    print(json_log_2)
