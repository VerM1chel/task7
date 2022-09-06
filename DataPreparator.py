import pandas as pd

class DataPreparator:
    def __init__(self, data):
        self.data = data

    def prepare_data(self):
        columnsToRename = {'0': 'error_code', '1': 'error_message', '2': 'severity', '3': 'log_location',
                           '4': 'mode', '5': 'model', '6': 'graphics', '7': 'session_id', '8': 'sdkv',
                           '9': 'test_mode', '10': 'flow_id', '11': 'flow_type', '12': 'sdk_date',
                           '13': 'publisher_id', '14': 'game_id', '15': 'bundle_id', '16': 'appv',
                           '17': 'language', '18': 'os', '19': 'adv_id', '20': 'gdpr', '21': 'ccpa',
                           '22': 'country_code', '23': 'date'}
        self.data.rename(columns=columnsToRename,
                    inplace=True)
        self.data = self.data[self.data['severity'] == 'Error']
        self.data['date'] = pd.to_datetime(self.data['date'], unit='s', origin='unix')
        self.data = self.data.groupby('bundle_id')

    def get_data(self):
        return self.data