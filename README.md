# 크레온 API 를 이용해 mysql 로 db 저장

- 일자별 주체 수급 현황 DB 
  - '개인', '외국인', '기관', '금융투자', '보험', '투신', '은행', '연기금', '사모펀드'

##

- 실행방법
  - (1) 기간 실행
    - Rate_DB_Updater.py 실행후
    - Market_DB_Updater.py 실행
  - (2) 매일 실행 (작업스케줄러로 실행)
    - Market_DB_Updater_daily.py

##
- 주의사항
  - 관리자 권한으로 실행!

##
- 설정 환경 <br> <br>
python 3.8 32bit <br> 
mariadb 10.6 64bit <br>
creon plus
