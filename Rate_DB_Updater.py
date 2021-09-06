from numpy import e
import pandas as pd
import pymysql, requests, time, re, decimal
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from threading import Timer
from Analyzer_Module import Analyzer_for_minute
import pymysql
import win32com.client

class Rate_DB_Updater:
    def __init__(self):
        """Constructor: Connect MariaDB with And Build Stockcode Dictionary"""
        self.conn = pymysql.connect(host='?', user='?', 
            password='****', db='?', charset='utf8') # Connection Object (self.conn) for cennect with sql server

        with self.conn.cursor() as curs: # Cursor Object (curs) for SQL Query
            sql = """
            CREATE TABLE IF NOT EXISTS company_info (
                code VARCHAR(20),
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (code))
            """
            curs.execute(sql) # excute SQL Query
            sql = """
            CREATE TABLE IF NOT EXISTS market_db_rate (
                code VARCHAR(20),
                date DATE,
                등락률 FLOAT(20),
                PRIMARY KEY (code, date))
            """
            curs.execute(sql) # excute SQL Query
        self.conn.commit() # Commited for reflected on DataBase

        self.m_db = Analyzer_for_minute.MinuteDB()

        self.codes = dict() # self.codes is dictionary
        self.update_comp_info() # execute def(update_comp_info)
        self.objStinfo = win32com.client.Dispatch("Dscbo1.StockWeek") # Creon Object(Rate)
        self.objPinfo = win32com.client.Dispatch("CpSysDib.StockChart") # Creon Object(Prices)


    
    def __del__(self):
        """Destructor: Disconnect MariaDB"""
        self.conn.close() # disconnect with sql server

    def read_krx_code(self):
        with self.conn.cursor() as curs:
            sql = """SELECT * FROM `marketdb`.`company_info`"""
            krx = pd.read_sql(sql, self.conn)
        return krx

    def update_comp_info(self):
            """Update StockCode to company_info Table and Save it to Dictionary"""
            sql = "SELECT * FROM company_info" # Select All From company_info
            df = pd.read_sql(sql, self.conn) # Read company_info Table with read_sql()
            for idx in range(len(df)): 
                self.codes[df['code'].values[idx]] = df['company'].values[idx] # ^df(DataFrame) to self.codes(Dictionary) \\ 005930 : 삼성전자

            with self.conn.cursor() as curs: # Cursor Object (curs) for SQL Query
                sql = "SELECT max(last_update) FROM company_info" # Bring Latest Date Data From company_info
                curs.execute(sql) # last date
                rs = curs.fetchone() # receive by tuple *****
                today = datetime.today().strftime('%Y-%m-%d') # today date (str)
                if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today: # if( no date or before today )
                    krx = self.read_krx_code() # krx <= DateFrame read_krx_code()
                    for idx in range(len(krx)): 
                        code = krx.code.values[idx] # Get code from KRX
                        company = krx.company.values[idx] # Get company from KRX
                        sql = f"REPLACE INTO company_info (code, company, last" \
                            f"_update) VALUES ('{code}', '{company}', '{today}')" # Put code company today to company_info
                        curs.execute(sql) # excute SQL Query
                        self.codes[code] = company # self.code(dictionary) code : company
                        tmnow = datetime.now().strftime('%Y-%m-%d %H:%M') # Right Now
                        print(f"[{tmnow}] #{idx + 1:04d} REPLACE INTO company_info " \
                            f"VALUES ({code}, {company}, {today})") # for Display on script
                    self.conn.commit() # Commited for reflected on DataBase
                    print('')


    def check_remain_time(self):
        """Evasion Creon Server Limit requests"""
        objCRT = win32com.client.Dispatch('CpUtil.CpCybos')
        remainTime = objCRT.LimitRequestRemainTime / 1000
        remainCount = objCRT.GetLimitRemainCount(1)
        #print('[{:04f}초 남음]  [{:04d} 개 남음]'.format(remainTime, remainCount))
        if remainCount <= 0:
            print(f"{remainCount}개 남음 {remainTime} 지연")
            print("15초당 60건으로 제한합니다.")
            print(' ')
            time.sleep(remainTime + 0.3)


    def update_market_db(self, df_data, idx, code, company):
        """Update CreonData to market_db Table"""
        with self.conn.cursor() as curs:
            for r in df_data.itertuples():
                sql = f"REPLACE INTO market_db_rate VALUES ('{code}', '{r.date}', {r.rate})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) > RATE REPLACE INTO daily_trends[OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'),
                                                                                           idx+1, company, code))


    def check_sign(self, A_code, date):
        self.check_remain_time()
        try:
            self.objPinfo.SetInputValue(0, A_code)
            self.objPinfo.SetInputValue(1, ord('1'))
            self.objPinfo.SetInputValue(2, date)
            self.objPinfo.SetInputValue(3, date)
            self.objPinfo.SetInputValue(4, 1)
            self.objPinfo.SetInputValue(5, (6))
            self.objPinfo.SetInputValue(6, ord('D'))
            self.objPinfo.SetInputValue(8, ord('0'))
            self.objPinfo.SetInputValue(9, ord('1'))
            self.objPinfo.SetInputValue(10, ord('1'))
            
            self.objPinfo.BlockRequest()
            diff = self.objPinfo.GetDataValue(0, 0)
            if (diff < 0):
                return -1
            elif (diff >= 0):
                return 1
        except Exception as e:
            print(f"{date} invalid date [break]")
            return 0


    def get_rate(self, idx, code, A_code):
        try:
            rate_df = pd.DataFrame()
            index = 0
            cnt = 1
            self.objStinfo.SetInputValue(0, A_code)
            self.check_remain_time()
            self.objStinfo.BlockRequest()
            while(True):
                if (index == 36):
                    self.check_remain_time()
                    self.objStinfo.BlockRequest()
                    index = 0
                date = self.objStinfo.GetDataValue(0, index)
                rate = self.objStinfo.GetDataValue(10, index)
                sign = self.check_sign(A_code, date)
                rate *= sign
                if (sign == 0 or date < 20160727):
                    print('rate_df complete!')
                    return rate_df
                else:
                    dict_data = {'code' : code, 'date' : str(date), 'rate' : rate}
                    rate_df = rate_df.append(dict_data, ignore_index=True)
                    print('[{}] #{:04d} #{:04d} {} ({}) > Getting rate_df[OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'),
                                                                                           idx + 1, cnt, date, code))
                cnt += 1
                index += 1

                    
        except Exception as e:
            print(f"get_rate : {e}")
            return

    def execute_updater(self, set_idx):
        """Excute the whole code"""        
        for idx, code in enumerate(self.codes):
            if (idx + 1 < set_idx):
                continue
            df_data = self.get_rate(idx, code, 'A' + code)
            if df_data is None:
                continue
            self.update_market_db(df_data, idx, code, self.codes[code])
        
    


if __name__ ==  '__main__':
    rdbu = Rate_DB_Updater()
    rdbu.execute_updater(865)