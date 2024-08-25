import pandas as pd
import sys
from datetime import date, datetime, timedelta
import json
from io import StringIO

today = datetime.today()

df = pd.read_csv(sys.argv[1])
df['past_due'] = (pd.to_datetime(today) - pd.to_datetime(df['due date'])).dt.days
df['days_open'] = (pd.to_datetime(today) - pd.to_datetime(df['create date'])).dt.days

df = df.loc[df['complete'] == False]
ele_dataframe = df.loc[df['owner'].str.contains("ele", case=False, na=False)]
merc_dataframe = df.loc[df['owner'].str.contains("merc", case=False, na=False)]
print(ele_dataframe)
