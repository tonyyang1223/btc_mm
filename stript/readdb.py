import sqlite3 as db
import datetime
file_db = '/home/shuai/gekko_dev/history/binance_0.1.db'
conn = db.connect(file_db)
c = conn.cursor()
sql= 'SELECT * FROM candles_USDT_BTC where start > :timestamp '
import pandas as pd
time_stamp = datetime.datetime(2019, 6, 26, 14, 6).timestamp()
data = pd.read_sql(sql=sql, con=conn,params={'timestamp': time_stamp})
print(data)
# config = {
#   'adapter':'sqlite',
#   "db":{
#     'database': '/home/shuai/gekko_dev/history/binance_0.1.db',
#     'max_idle':5*60,
#     }
   
# }
# import os
# import sys
# sys.path.append('../lib')
# from lib import db
# import datetime

# db.setup(config['db'],adapter=config['adapter'],slave=True)
# time_stamp = datetime.datetime(2019, 6, 26, 14, 6).timestamp()
# ret = db.select("candles_USDT_BTC").condition('start',"1561529160.0",">").execute()
print(ret)