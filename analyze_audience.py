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
"""
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
plt.show()

