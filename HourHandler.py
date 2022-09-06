import logging
from datetime import timedelta, datetime

import pandas as pd


class HourHandler:
    def __init__(self, data):
        self.data = data

    def batched_alert(self):
        data_by_hours = self.data.resample('H', on='date').size()
        data_by_hours = data_by_hours[data_by_hours > 10]
        for row in data_by_hours.items():
            print(f"{row[0][1] + timedelta(hours=1)}: received {row[1]} errors during last hour")
            logging.warning(f"{row[0][1] + timedelta(hours=1)}: received {row[1]} errors during last hour")

    def streaming_alert(self):
        df_by_minutes = pd.DataFrame(self.data.resample('1S', on='date').size()).reset_index()
        df_by_minutes = df_by_minutes[df_by_minutes.date > pd.Timestamp(datetime.now()) - timedelta(hours=1)]
        df_by_minutes = df_by_minutes[df_by_minutes.date < pd.Timestamp(datetime.now())]
        print(f"{pd.Timestamp(datetime.now())}: received {sum(df_by_minutes[0])} errors during last hour")
        logging.warning(f"{pd.Timestamp(datetime.now())}: received {sum(df_by_minutes[0])} errors during last hour")


