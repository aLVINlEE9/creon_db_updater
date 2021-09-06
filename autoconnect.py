from pywinauto import application
import time
import os

def auto_connect():
    os.system('taskkill /IM coStarter* /F /T')
    os.system('taskkill /IM CpStart* /F /T')
    os.system('taskkill /IM DibServer* /F /T')
    os.system('wmic process where "name like \'%coStarter%\'" call terminate')
    os.system('wmic process where "name like \'%CpStart%\'" call terminate')
    os.system('wmic process where "name like \'%DibServer%\'" call terminate')
    time.sleep(5)        

    app = application.Application()
    app.start('C:\CREON\STARTER\coStarter.exe /prj:cp /id:아이디 /pwd:비밀번호 /pwdcert:공인인증서 비밀번호 /autostart')
    time.sleep(60)
    
auto_connect()