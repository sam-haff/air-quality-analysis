import pandas as pd
import datetime

df = pd.read_parquet('gs://kestra-de-main-bucket/aq/raw/measurements/slovakia/2024/05.parquet')
#df = pd.read_parquet('./tmp/06.parquet')
#df = df.convert_objects(convert_dates='coerce')
#df = df.set_index('datetime')
#df.datetime = df.datetime.dt.tz_convert('UTC')
df.datetime = df.datetime.pipe(pd.to_datetime).dt.tz_convert('UTC').apply(pd.Timestamp.isoformat) + 'Z'
#df.datetime = df.datetime.pipe(pd.to_datetime).dt.tz_convert('UTC').apply(lambda x: datetime.datetime.strftime(x, '%y-%m-%dT%H:%M:%S.00Z'))#pd.Timestamp.isoformat) + 'Z'
#df['datetime'] = df.datetime.map(lambda x: datetime.datetime.strftime(x, '%y%m%dT%H:%M%SZ'))
#df['datetime'] = df.datetime.map(lambda x: datetime.datetime.isoformat(x))
#col = pd.to_datetime(df['datetime'])
#col = col.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
#df['datetime'] = col
#df['datetime'].apply(
#        lambda x: pd.datetools.parse(x).strftime('%Y-%m-%dT%H:%M:%SZ'))
print(df)
