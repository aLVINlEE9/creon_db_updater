from numpy import e
import pandas as pd
import pymysql, requests, time, re, decimal
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from threading import Timer
import pymysql
import win32com.client

class Market_DB_Updater:
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
            CREATE TABLE IF NOT EXISTS minute_db (
                code VARCHAR(20),
                date DATE,
                time TIME,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20), 
                pvolume BIGINT(20),
                acc_sell BIGINT(20),
                acc_buy BIGINT(20),
                PRIMARY KEY(code, date, time))
            """
            curs.execute(sql) # excute SQL Query
        self.conn.commit() # Commited for reflected on DataBase

        self.codes = dict() # self.codes is dictionary
        self.update_comp_info() # execute def(update_comp_info)
        self.objStinfo = win32com.client.Dispatch("Dscbo1.StockWeek") # Creon Object(Rate)

    
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



    def read_creon_data(self, idx, date, code, A_code, company, index):
        """Get MacketData From data each day each code"""
        try:
            print(f'[{idx}] Getting Creon Data : {date}, {A_code}, {company}') 
            df = pd.DataFrame()          
            objmin = win32com.client.Dispatch("CpSysDib.StockChart")
                
            objmin.SetInputValue(0, A_code)
            objmin.SetInputValue(1, ord('1'))
            objmin.SetInputValue(2, date)
            objmin.SetInputValue(3, date)
            objmin.SetInputValue(5, [0, 1, 2, 3, 4, 5, 9, 10, 11])
            objmin.SetInputValue(6, ord('m'))
            objmin.SetInputValue(7, 1)
            objmin.SetInputValue(9, ord('1'))

            objmin.BlockRequest()

            cnt = objmin.GetHeaderValue(3)
            
            for i in range(cnt):
                try:
                    dict_data = {'date': date, 'time': objmin.GetDataValue(1, i), 'open': objmin.GetDataValue(2, i),
                            'high': objmin.GetDataValue(3, i), 'low': objmin.GetDataValue(4, i),
                            'close': objmin.GetDataValue(5, i),'pvolume': objmin.GetDataValue(6, i) / 10000, 
                            'acc_sell': objmin.GetDataValue(7, i),'acc_buy': objmin.GetDataValue(8, i)}
                    print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.
                        format(date, company, code, i, cnt), end="\r")
                    df = df.append(dict_data, ignore_index=True)
                except Exception as e:
                    print('Exception occured :', str(e), i)
                    dict_data = {'date': date, 'time': 0, 'open': 0, 'high': 0, 'low': 0,
                            'close': 0,'pvolume': 0, 'acc_sell': 0,'acc_buy': 0}
                    df = df.append(dict_data, ignore_index=True)

            df = df.dropna()
            df[['open', 'high', 'low', 'close', 'pvolume', 'acc_sell', 'acc_buy']] = df[['open', 'high',
                                                                            'low', 'close', 'pvolume', 'acc_sell', 'acc_buy']].astype(int)
            df = df[['date', 'time', 'open', 'high', 'low', 'close', 'pvolume', 'acc_sell', 'acc_buy']]
        
        except Exception as e:
            print('Exception occured(read_creon_data) :', str(e))


        return df


    def update_market_db(self, df_data, idx, code, company):
        """Update CreonData to market_db Table"""
        with self.conn.cursor() as curs:
            for r in df_data.itertuples():
                sql = f"REPLACE INTO minute_db VALUES ('{code}', "\
                    f"'{r.date}', {r.time}, {r.open}, {r.high}, {r.low}, {r.close}, {r.pvolume}, "\
                    f"{r.acc_sell}, {r.acc_buy})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO minute_db [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'),
                                                                                           idx+1, company, code, len(df_data)))


    def covert_to_num(self, date):
        date = str(date)
        date = re.sub("-", "", date)
        return date


    def check_remain_time(self):
        """Evasion Creon Server Limit requests"""
        objCRT = win32com.client.Dispatch('CpUtil.CpCybos')
        remainTime = objCRT.LimitRequestRemainTime / 1000
        remainCount = objCRT.GetLimitRemainCount(1)
        #print(f"{remainTime}초 {remainCount}개")
        if remainCount <= 3:
            print(f"{remainCount}개 남음")
            print(f"{remainTime} 지연")
            print("15초당 60건으로 제한합니다.")
            time.sleep(15.5)


    def execute_updater(self):
        """Excute the whole code"""
        self.update_comp_info()
        s_datepram = date(2021, 8, 7) # 조회 시작일
        e_datepram = date(2021, 8, 18) # 조회 종료일
        enddate = int(self.covert_to_num(e_datepram))
        index = 0
        self.objStinfo.SetInputValue(0, 'A005930')
        self.objStinfo.BlockRequest()
        while(s_datepram <= e_datepram):
            
            if (index == 36):
                self.objStinfo.BlockRequest()
                index = 0
            get_date = self.objStinfo.GetDataValue(0, index)
            want_date = int(self.covert_to_num(e_datepram))
            #print(index, get_date, want_date)
            if (get_date != want_date):
                if (get_date < want_date):
                    e_datepram -= timedelta(days=1)
                    continue
                elif (get_date > want_date):
                    index += 1
                    continue
            for idx, code in enumerate(self.codes):
                cvt_date = self.covert_to_num(e_datepram)
                df_data = self.read_creon_data(idx, cvt_date, code, 'A' + code, self.codes[code], index)
                self.check_remain_time()
                if df_data is None:
                    continue
                self.update_market_db(df_data, idx, code, self.codes[code])
            index += 1
            e_datepram -= timedelta(days=1)

        


if __name__ ==  '__main__':
    mdbu = Market_DB_Updater()
    mdbu.execute_updater()