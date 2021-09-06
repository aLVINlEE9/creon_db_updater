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
            CREATE TABLE IF NOT EXISTS market_db (
                code VARCHAR(20),
                date DATE,
                시가 BIGINT(20),
                종가 BIGINT(20),
                등락률 FLOAT(20),
                전일대비 BIGINT(20),
                거래대금 BIGINT(20),
                시가총액 BIGINT(20),
                개인 BIGINT(20),
                외국인 BIGINT(20),
                기관 BIGINT(20),
                금융투자 BIGINT(20),
                보험 BIGINT(20),
                투신 BIGINT(20),
                은행 BIGINT(20),
                연기금 BIGINT(20),
                사모펀드 BIGINT(20),
                PRIMARY KEY (code, date))
            """
            curs.execute(sql) # excute SQL Query
        self.conn.commit() # Commited for reflected on DataBase

        self.codes = dict() # self.codes is dictionary
        self.update_comp_info() # execute def(update_comp_info)

    
    def __del__(self):
        """Destructor: Disconnect MariaDB"""
        self.conn.close() # disconnect with sql server


    def read_krx_code(self):
        """Read Stock Codes from KRX and convert to DataFrame and Return it"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='\
            'download&searchType=13' # URL where StockCodes file At
        krx = pd.read_html(url, header=0)[0] # Read xls file with read_html()
        krx = krx[['종목코드', '회사명']] # Choose Necessary Column
        krx = krx.rename(columns={'종목코드':'code', '회사명':'company'}) # Rename it to English
        krx.code = krx.code.map('{:06d}'.format) # Map the StockCode
        return krx # return KRX(DataFrame)


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


    def read_creon_data(self, idx, date, code, A_code, company):
        """Get MacketData From data each day each code"""
        print(f'[{idx}] Getting Creon Data : {date}, {A_code}, {company}')
        try:
            
            objTinfo = win32com.client.Dispatch("CpSysDib.CpSvr7254") # Creon Object(Trends)
            objPinfo = win32com.client.Dispatch("CpSysDib.StockChart") # Creon Object(Prices)
            objStinfo = win32com.client.Dispatch("Dscbo1.StockWeek") # Creon Object(Rate)


            objTinfo.SetInputValue(0, A_code)
            objTinfo.SetInputValue(1, 0)
            objTinfo.SetInputValue(2, date)
            objTinfo.SetInputValue(3, date)
            objTinfo.SetInputValue(4, ord('0'))
            objTinfo.SetInputValue(5, 0)
            objTinfo.SetInputValue(6, ord('2'))

            objPinfo.SetInputValue(0, A_code)
            objPinfo.SetInputValue(1, ord('1'))
            objPinfo.SetInputValue(2, date)
            objPinfo.SetInputValue(3, date)
            objPinfo.SetInputValue(4, 5)
            objPinfo.SetInputValue(5, (2, 5, 6, 9, 13, 37))
            objPinfo.SetInputValue(6, ord('D'))
            objPinfo.SetInputValue(8, ord('0'))
            objPinfo.SetInputValue(9, ord('1'))
            objPinfo.SetInputValue(10, ord('1'))

            objStinfo.SetInputValue(0, A_code)


            objTinfo.BlockRequest()
            objPinfo.BlockRequest()
            objStinfo.BlockRequest()


            rate = objStinfo.GetDataValue(10, 0),
            if (objPinfo.GetDataValue(2, 0) < 0):
                rate *= -1

            dict_data = {'date': date, '시가': objPinfo.GetDataValue(0, 0), '종가': objPinfo.GetDataValue(1, 0),
                    '등락률' : rate,
                    '전일대비': objPinfo.GetDataValue(2, 0), '거래대금': objPinfo.GetDataValue(3, 0) / 1000000,
                    '시가총액': objPinfo.GetDataValue(4, 0) / 100000000,
                    '개인': objTinfo.GetDataValue(1, 0), '외국인': objTinfo.GetDataValue(2, 0),
                    '기관': objTinfo.GetDataValue(3, 0), '금융투자': objTinfo.GetDataValue(4, 0),
                    '보험': objTinfo.GetDataValue(5, 0), '투신': objTinfo.GetDataValue(6, 0),
                    '은행': objTinfo.GetDataValue(7, 0), '연기금': objTinfo.GetDataValue(8, 0),
                    '사모펀드': objTinfo.GetDataValue(9, 0)}
        
        except Exception as e:
            print('Exception occured :', str(e))
            print(f'================Exception at {A_code} / {date}================')

            dict_data = {'date': date, '시가': 0, '종가': 0, '등락률' : 0, '전일대비': 0, '거래대금': 0,
                    '시가총액': 0, '개인': 0, '외국인': 0, '기관': 0, '금융투자': 0,
                    '보험': 0, '투신': 0, '은행': 0, '연기금': 0, '사모펀드': 0}
        return dict_data


    def update_market_db(self, df_data, idx, code, company):
        """Update CreonData to market_db Table"""
        with self.conn.cursor() as curs:
            for r in df_data.itertuples():
                sql = f"REPLACE INTO market_db VALUES ('{code}', "\
                    f"'{r.date}', {r.시가}, {r.종가}, {r.등락률}, {r.전일대비}, {r.거래대금}, {r.시가총액}, "\
                    f"{r.개인}, {r.외국인}, {r.기관}, {r.금융투자}, {r.보험}, {r.투신}, {r.은행}, {r.연기금}, {r.사모펀드})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_trends [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'),
                                                                                           idx+1, company, code, len(df_data)))

    
    def convert_to_DataFrame(self, dict_data):
        df_data = pd.DataFrame()
        df_data = df_data.append(dict_data, ignore_index=True)
        df_data = df_data.dropna()
        df_data[['시가', '종가', '전일대비', '거래대금', '시가총액', '개인',
             '외국인', '기관', '금융투자', '보험', '투신', '은행', '연기금', '사모펀드']] = df_data[['시가', '종가',
             '전일대비', '거래대금', '시가총액', '개인', '외국인', '기관', '금융투자', '보험', '투신',
             '은행', '연기금', '사모펀드']].astype(int)
        df_data = df_data[['date', '시가', '종가', '등락률', '전일대비', '거래대금', '시가총액', '개인',
             '외국인', '기관', '금융투자', '보험', '투신', '은행', '연기금', '사모펀드']]
        return df_data


    def covert_to_num(self, date):
        date = str(date)
        date = re.sub("-", "", date)
        return date


    def check_remain_time(self):
        """Evasion Creon Server Limit requests"""
        objCRT = win32com.client.Dispatch('CpUtil.CpCybos')
        remainTime = objCRT.LimitRequestRemainTime / 1000
        remainCount = objCRT.GetLimitRemainCount(1)

        if remainCount <= 0:
            print("15초당 60건으로 제한합니다.")
            time.sleep(remainTime)


    def execute_updater(self):
        """Excute the whole code"""
        self.update_comp_info()
        date_info = datetime.today()
        datepram = datetime.today().strftime("%Y-%m-%d") #오늘 날짜
        if datetime.weekday(date_info) >= 5:
            print("오늘은 주말 입니다")
            return 0
        for idx, code in enumerate(self.codes):
            print("{datepram} : 데이터 수집 시작")
            cvt_date = self.covert_to_num(datepram)
            dict_data = self.read_creon_data(idx, cvt_date, code, 'A' + code, self.codes[code])
            df_data = self.convert_to_DataFrame(dict_data)
            self.check_remain_time()
            if df_data is None:
                continue
            self.update_market_db(df_data, idx, code, self.codes[code])

        


if __name__ ==  '__main__':
    mdbu = Market_DB_Updater()
    mdbu.execute_updater()