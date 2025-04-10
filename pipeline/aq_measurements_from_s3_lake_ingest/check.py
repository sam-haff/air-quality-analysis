import pandas as pd
import numpy as np
import datetime

df = pd.read_parquet('gs://kestra-de-main-bucket/aq/raw/measurements/slovakia/2024/11.parquet')
print(df.datetime)
df['datetime'] = pd.to_datetime(df["datetime"]).apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))

print(df.datetime)
print(df)
#1732665600000000000

#df = pd.read_parquet('./tmp/06.parquet')
#df = df.convert_objects(convert_dates='coerce')
#df = df.set_index('datetime')
#df.datetime = df.datetime.dt.tz_convert('UTC')
#df.datetime = pd.to_datetime(df.datetime).dt.tz_convert('UTC').apply(pd.Timestamp.isoformat) + 'Z'
#df['datetime'] = pd.to_datetime(df['datetime'])
#df['datetime'] = df.datetime.values.astype(np.int64)
#df.datetime =  pd.to_datetime(df.datetime).dt.tz_convert('UTC').apply(lambda x: datetime.datetime.strftime(x, '%y-%m-%dT%H:%M:%S.00Z'))#pd.Timestamp.isoformat) + 'Z'
#df['datetime'] = df.datetime.map(lambda x: datetime.datetime.strftime(x, '%y%m%dT%H:%M%SZ'))
#df['datetime'] = df.datetime.map(lambda x: datetime.datetime.isoformat(x))
#col = pd.to_datetime(df['datetime'])
#col = col.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
#df['datetime'] = col
#df['datetime'].apply(
#        lambda x: pd.datetools.parse(x).strftime('%Y-%m-%dT%H:%M:%SZ'))
#print(df)
