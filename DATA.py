
import json
import pandas as pd
from datetime import datetime, date
from io import StringIO
import sys

# Setup logging

week_number = datetime.utcnow().isocalendar()[1]
today = datetime.today()
last_week = week_number - 1 
# Vendor Email - just for testing
merc_email = ["ipants@me.com"]
ele_email = ["ipants@me.com"]
toAddress_recipient = "Matt"
send_date = date.today().strftime("%B %d, %Y")


df = pd.read_csv(sys.argv[1])

# SET VARIABLES
wk_create= pd.to_datetime(df['create date'])
wk_complete = pd.to_datetime(df['date completed'])

# NEW COLUMNS
df['week_created'] = wk_create.dt.isocalendar().week
df['week_completed'] = wk_complete.dt.isocalendar().week


df_open = df[~df['complete']]
df_open['days_opens'] = (pd.to_datetime(today) -  pd.to_datetime(df_open['create date'])).dt.days
df_completed = df[df['complete']]
df_completed['days_to_complete'] = (pd.to_datetime(today) - pd.to_datetime(df_completed['date completed'])).dt.days

df_count_complete = len(df[df['complete']])
df_completed_last_week = df_completed[df_completed['week_completed'] == last_week]


print(df_open)

