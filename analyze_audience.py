import sys

import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_table(f'data/src/{2013}/part-00000', index_col=False, header=None,
                   names=['id', 'biketype', 'starttime', 'endtime', 'startstation', 'endstation',
                          'startlat', 'startlng', 'endlat', 'endlng', 'member_casual', 'gender',
                          'age',
                          'bikeid'], sep=',')
"""
for i in range(2014, 2020):
    df2 = pd.read_table(f'data/src/{i}/part-00000', index_col=False, header=None,
                        names=['id', 'biketype', 'starttime', 'endtime', 'startstation',
                               'endstation',
                               'startlat', 'startlng', 'endlat', 'endlng', 'member_casual',
                               'gender', 'age',
                               'bikeid'], sep=',')
    df = pd.concat([df, df2])

age_payment = df[['age', 'member_casual']]
age_payment = age_payment.dropna()
age_payment_cust = age_payment[age_payment['member_casual'] == 'Customer']
age_payment_sub = age_payment[age_payment['member_casual'] == 'Subscriber']

age_bins_sub = pd.cut(age_payment_sub['age'],
                      [0, 15, 18, 21, 24, 27, 30, 35, 40, 45, 50, 55, 60, 70, 90,
                       age_payment_sub['age'].max()],
                      include_lowest=True)
age_bins_cust = pd.cut(age_payment_cust['age'],
                       [0, 15, 18, 21, 24, 27, 30, 35, 40, 45, 50, 55, 60, 70, 90,
                        age_payment_cust['age'].max()],
                       include_lowest=True)

age_payment_sub['age_bins'] = age_bins_sub
age_payment_cust['age_bins'] = age_bins_cust
age_payment_sub = age_payment_sub.groupby('age_bins')['member_casual'].count()
age_payment_cust = age_payment_cust.groupby('age_bins')['member_casual'].count()

plt.figure(figsize=(10, 5))
age_payment_sub.plot()
age_payment_cust.plot()
plt.legend(['Подписчики', 'Работяги'])
# plt.show()
age_time = df[['age', 'starttime', 'endtime']]
age_time['starttime'] = pd.to_datetime(age_time['starttime'])
age_time['endtime'] = pd.to_datetime(age_time['endtime'])
age_time['duration'] = round((age_time['endtime'] - age_time['starttime']).dt.seconds / 60)
age_time2 = age_time[['age', 'duration']]

age_bins = pd.cut(age_time2['age'],
                  [0, 15, 18, 21, 24, 27, 30, 35, 40, 45, 50, 55, 60, 70, 90,
                   age_time2['age'].max()],
                  include_lowest=True)
age_time2['age_bins'] = age_bins
age_time2 = age_time2.dropna().groupby('age_bins')['duration'].mean()
age_time2.plot()
# plt.show()

plt.figure(figsize=(10, 5))
age_bins = pd.cut(age_time['age'],
                  [0, 15, 18, 21, 24, 27, 30, 35, 40, 45, 50, 55, 60, 70, 90, 150,
                   age_time['age'].max()],
                  include_lowest=True)
age_time['starttime'] = age_time['starttime'].dt.hour + age_time['starttime'].dt.minute / 60
age_time['age_bins'] = age_bins
age_time = age_time[['age_bins', 'starttime']]
age_time = age_time.dropna().groupby('age_bins')['starttime'].mean()
plt.ylabel('Время начала поездки, в часах суток')
age_time.plot()
# plt.show()

age_gender = df[['age', 'gender']]
print(age_gender['age'].max())
age_gender['age_bins'] = pd.cut(age_gender['age'],
                                [0, 15, 18, 21, 24, 27, 30, 35, 40, 45, 50, 55, 60, 70, 90, 150],
                                include_lowest=True)
age_gender = age_gender.dropna()
age_male = age_gender[age_gender['gender'] == 'Male'].groupby(['age_bins'])['gender'].count()
age_female = age_gender[age_gender['gender'] == 'Female'].groupby(['age_bins'])['gender'].count()
age_male.plot()
age_female.plot()
plt.legend(['Мужчины', 'Женщины'])
# plt.show()

stations = df[['startstation', 'id']]
stations['startstation'] = stations['startstation'].dropna()
stations = stations.groupby('startstation').count().sort_values(by='id', ascending=False)
FIRST_N = 50
stations = stations.head(FIRST_N).axes[0].values.tolist()
print(stations)
startstation_age = df[['startstation', 'age']]
startstation_age = startstation_age.dropna().groupby('startstation').mean()
print(startstation_age)

data = {'station': [], 'age': []}
for i in range(FIRST_N):
    # print(startstation_age.axes[0].values)
    data['age'].append(startstation_age.loc[stations[i]][0])
    data['station'].append(stations[i])
df2 = pd.DataFrame(data).set_index('station')
plt.figure(figsize=(20, 5))
plt.plot(df2)
# plt.show()

age_id = df[['age', 'id']]
age_id = age_id.dropna().groupby('age').count()
a = age_id.axes[0].values
# plt.scatter(a, age_id)
# plt.show()

from pyspark import SparkConf, SparkContext


def parse(line):
    info = line.split(',')
    # float(info[-2]) - возраст
    # float(info[-3]) - пол
    return [((info[-3], info[-2]), 1)]


conf = SparkConf().setAppName('test').setMaster('local')
sc = SparkContext(conf=conf)
df0 = sc.textFile(f'data/src/{2013}/part-00000')
for i in range(2014, 2020):
    df0 = df0.union(sc.textFile(f'data/src/{i}/part-00000'))

df0 = df0.flatMap(parse).filter(lambda tup: tup[0][0] != '' and tup[0][1] != '').\
    map(lambda tup: ((tup[0][0], float(tup[0][1])), 1)).reduceByKey(lambda v1, v2: v1 + v2)\
    .collect()
summ = 0
for i in df0:
    summ += i[1]

data = {'Gender': [], 'Age': [], 'Num': []}
for i in df0:
    perc = round(i[1] / summ * 100, 1)
    if perc > 0:
        data['Gender'].append(i[0][0])
        data['Age'].append(i[0][1])
        data['Num'].append(perc)

from pprint import pprint
pprint(data)
print(summ)
plt.figure(figsize=(10, 5))
plt.ylabel('% польз. возраста от всех польз.')
df3 = pd.DataFrame(data)
df3_male = df3[df3['Gender'] == 'Male']
df3_female = df3[df3['Gender'] == 'Female']
df3_male = df3_male.groupby('Age')['Num'].sum()
df3_female = df3_female.groupby('Age')['Num'].sum()
df3_female.plot()
df3_male.plot()
# plt.show()
"""
import numpy as np
"""
srednee_dur = []
for i in range(2013, 2024):
    df = pd.read_table(f'data/src/{i}/part-00000', index_col=False, header=None,
                       names=['id', 'biketype', 'starttime', 'endtime', 'startstation',
                              'endstation',
                              'startlat', 'startlng', 'endlat', 'endlng', 'member_casual', 'gender',
                              'age',
                              'bikeid'], sep=',')
    df = df[['endtime', 'starttime', 'age']]
    df['age_bins'] = pd.cut(df['age'],
                  [0, 15, 18, 21, 24, 27, 30, 35, 40, 45, 50, 55, 60, 70, 90, 150],
                  include_lowest=True)
    df['starttime'] = pd.to_datetime(df['starttime'])
    df['endtime'] = pd.to_datetime(df['endtime'])
    df['duration'] = round((df['endtime'] - df['starttime']).dt.seconds / 60)
    print(df)
    df = df.dropna().groupby('age_bins')['duration'].mean()
    df_durs = df.values
    df_durs = df_durs[~np.isnan(df_durs)]
    srednee_dur.append(df_durs.sum() / len(df_durs))
    df.plot()
#plt.show()
#plt.legend([f'{i}' for i in range(2013, 2020)])
plt.xlabel('Год')
plt.ylabel('Средняя продолжительность поездок в мин.')
plt.plot(range(2013, 2024), srednee_dur)
#plt.show()
"""
from pyspark import SparkConf, SparkContext
from datetime import datetime, timedelta


def parse2(line):
    info = line.split(',')
    dur = (datetime.fromisoformat(info[3]) - datetime.fromisoformat(info[2])).seconds / 60
    return [(dur, {'id': int(info[0]), 'starttime': info[2], 'endtime': info[3], 'gender': info[-3],
             'age': info[-2], 'duration': dur})]


conf = SparkConf().setAppName('test').setMaster('local')
sc = SparkContext(conf=conf)
df0 = sc.textFile(f'data/src/{2013}/part-00000')
for i in range(2014, 2020):
    df0 = df0.union(sc.textFile(f'data/src/{i}/part-00000'))
df0 = df0.flatMap(parse2).sortByKey(ascending=False).top(10)
print(df0)