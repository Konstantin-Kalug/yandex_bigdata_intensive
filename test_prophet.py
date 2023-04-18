from pyspark import SparkConf, SparkContext
import os
import datetime
import shutil
import pandas as pd
import numpy as np
import requests
import networkx as nx
from IPython.display import Image
import matplotlib.pyplot as plt
from math import floor
from prophet import Prophet

"""
ПРОГНОЗЫ
"""
# ===========
# Прогноз кол-ва поездок членов и не членов

members = []
casuals = []
for year in range(2013, 2024):
    df = pd.read_table(f'data/src/{year}/part-00000', index_col=False, header=None, names=['id', 'biketype', 'starttime', 'endtime', 'startstation', 'endstation', 'startlat', 'startlng', 'endlat', 'endlng', 'member_casual', 'gender', 'age', 'bikeid'], sep=',')
    casuals.append(len(df.loc[(df['member_casual'] == 'Customer') | (df['member_casual'] == 'casual')]))
    members.append(len(df.loc[(df['member_casual'] == 'Subscriber') | (df['member_casual'] == 'member')]))
plt.plot(range(2013, 2024), casuals)
plt.plot(range(2013, 2024), members)
plt.xlabel('Года наблюдений')
plt.ylabel('Количество поездок, млн.')
plt.legend(['Нечлены', 'Члены'])
plt.title('Динамическое изменение количества членов и нечленов')
plt.show()

# ===========