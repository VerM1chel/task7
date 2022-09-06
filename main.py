from DataPreparator import DataPreparator
from MinuteHandler import MinuteHandler
from HourHandler import HourHandler

import logging
import sys

import pandas as pd


def save_logs(filename, func):
        with open(filename, 'w') as f:
            sys.stdout = f
            data = func()
            if data is not None:
                print(data)
            sys.stdout = sys.stdout


def main():
    data = pd.read_csv('data.csv')
    dp = DataPreparator(data)
    dp.prepare_data()
    mh = MinuteHandler(dp.get_data())
    save_logs("reports/minuteBatched.log", mh.batched_alert)
    save_logs("reports/minuteStreaming.log", mh.streaming_alert)
    hh = HourHandler(dp.get_data())
    save_logs("reports/hourBatched.log", hh.batched_alert)
    save_logs("reports/hourStreaming.log", hh.streaming_alert)


main()