import pandas as pd
from prophet import Prophet
from datetime import datetime
import pystan
"""
ПРОГНОЗЫ
"""
# ===========
# Прогноз кол-ва поездок членов и не членов

members = []
casuals = []
st = 2013
end = 2023
for year in range(st, end):
    df = pd.read_table(f'data/src/{year}/part-00000', index_col=False, header=None, names=['id', 'biketype', 'starttime', 'endtime', 'startstation', 'endstation', 'startlat', 'startlng', 'endlat', 'endlng', 'member_casual', 'gender', 'age', 'bikeid'], sep=',')
    casuals.append(len(df.loc[(df['member_casual'] == 'Customer') | (df['member_casual'] == 'casual')]))
    members.append(len(df.loc[(df['member_casual'] == 'Subscriber') | (df['member_casual'] == 'member')]))
    print(year)
df_casuals = pd.DataFrame(data={'y': casuals, 'ds': [datetime(year=i, month=1, day=1) for i in
                                                     range(st, end)]})
df_members = pd.DataFrame(data={'y': members, 'ds': [datetime(year=i, month=1, day=1) for i in
                                                     range(st, end)]})
print(df_casuals)
m_casuals = Prophet()
m_casuals.fit(df_casuals)
future_casuals = m_casuals.make_future_dataframe(periods=5, freq='Y')
forecast_casuals = m_casuals.predict(future_casuals)
print(forecast_casuals[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())

m_members = Prophet()
m_members.fit(df_casuals)
future_members = m_members.make_future_dataframe(periods=5, freq='Y')
forecast_members = m_casuals.predict(future_members)
print(forecast_members[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
# ===========
