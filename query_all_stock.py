import baostock as bs
import pandas as pd
import datetime
import time
from sqlalchemy import create_engine

def download_data(date):
    # 获取指定日期的指数、股票数据
    stock_rs = bs.query_all_stock(date)
    stock_df = stock_rs.get_data()
    data_df = pd.DataFrame()

    start = time.time()
    count = 0

    # 交易日
    try: 
        for code in stock_df["code"]:
            count = count + 1
            end = time.time()
            timestamp = end - start
            print("Downloading : Index " + str(count)  + ", " + date_str + ", " + code + ", " + str(round(timestamp,2)) + "秒")
            k_rs = bs.query_history_k_data_plus(code, "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST", date, date)
            data_df = data_df.append(k_rs.get_data())
       
        #数据存入csv文件
        data_df.to_csv("D:\\stockData_" + date +".csv", encoding="gbk", index=False)
        #数据存入mssql
        data_df.to_sql('history_k_data', con, if_exists='append')    
        
    # 非交易日    
    except:
    	print(date+"非交易日")


if __name__ == '__main__':
	# MSSQL 参数
    DRIVER = "ODBC Driver 17 for SQL Server"
    USERNAME = "sa"
    PSSWD = "123456"
    SERVERNAME = ""
    INSTANCENAME = "\TEST_SQLSERVER"
    DB = "StockA"
    TABLE = "history_k_data"

    #通过 sqlalchemy 存入SQL Server
    con = create_engine(
    f"mssql+pyodbc://{USERNAME}:{PSSWD}@{SERVERNAME}{INSTANCENAME}/{DB}?driver={DRIVER}", fast_executemany=True
    )
	
	#最近8000天的K线数据
    begin_date = (datetime.datetime.now() - datetime.timedelta(days = 8000)).strftime("%Y-%m-%d")
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(time.strftime('%Y-%m-%d',time.localtime(time.time())), "%Y-%m-%d")
    
    bs.login()	
    # 获取指定日期全部股票的日K线数据	
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")        
        download_data(date_str)
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    bs.logout()


    
