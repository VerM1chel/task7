import logging
from datetime import timedelta, datetime

import pandas as pd


class MinuteHandler:
    def __init__(self, data):
        self.data_by_minutes = data

    def batched_alert(self):
        data_by_minutes = self.data_by_minutes.resample('60S', on='date').size()
        data_by_minutes = data_by_minutes[data_by_minutes > 10]
        for row in data_by_minutes.items():
            print(f"{row[0][1] + timedelta(minutes=1)}: received {row[1]} errors during last minute")
            logging.warning(f"{row[0][1] + timedelta(minutes=1)}: received {row[1]} errors during last minute")

    def streaming_alert(self):
        df_by_minutes = pd.DataFrame(self.data_by_minutes.resample('1S', on='date').size()).reset_index()
        df_by_minutes = df_by_minutes[df_by_minutes.date > pd.Timestamp(datetime.now()) - timedelta(minutes=1)]
        df_by_minutes = df_by_minutes[df_by_minutes.date < pd.Timestamp(datetime.now())]
        print(f"{pd.Timestamp(datetime.now())}: received {sum(df_by_minutes[0])} errors during last minute")
        logging.warning(f"{pd.Timestamp(datetime.now())}: received {sum(df_by_minutes[0])} errors during last minute")
