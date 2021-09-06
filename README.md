# 크레온 API 를 이용해 한국 주식 mysql 로 db 저장 <br>(Korean Stock DB updater with Creon API)

- 1분봉 데이터 DB (최대 2년)

- 5분봉 데이터 DB (최대 5년)

- 일자별 주체 수급 현황 DB 
  - '개인', '외국인', '기관', '금융투자', '보험', '투신', '은행', '연기금', '사모펀드'

- DataFrame 분석툴
##

- 실행방법
  - 일자별 주체 수급 현황 DB 기간 실행
    - Rate_DB_Updater.py 실행후
    - Market_DB_Updater.py 실행
  - 일자별 주체 수급 현황 DB 매일 실행 (작업스케줄러로 실행)
    - Market_DB_Updater_daily.py
  - 1분봉 데이터 DB (최대 2년) 기간 실행
    - Minute_DB_Updater.py
  - 1분봉 데이터 DB (최대 2년) 매일 실행 (작업스케줄러로 실행)
    - Minute_DB_Updater_daily.py
  - 5분봉 데이터 DB (최대 5년) 기간 실행
    - Minute_5_DB_Updater.py

  - Analyzer_Mondule
    - minute
    ```python
       from Analyzer_Module import Analyzer_for_minute
    ```
    - daliy
    ```python
       from Analyzer_Module import Analyzer_for_db
    ```
##
- 주의사항
  - 관리자 권한으로 실행!

##
- 설정 환경 <br> <br>
python 3.8 32bit <br> 
mariadb 10.6 64bit <br>
creon plus
