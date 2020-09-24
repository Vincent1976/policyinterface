import pandas as pd
import pymssql
import datetime
import traceback
import smtplib
from email.header import Header
from email.mime.text import MIMEText
import pymysql
import math
import pymssql
import datetime
import traceback
import smtplib
from email.header import Header
from email.mime.text import MIMEText
import pymysql

conn = pymysql.connect(host="120.133.142.144",user="root",password="sate1llite",database="sanzhi2019",charset='utf8',port=15336)
cursor = conn.cursor() 
sql="select insuredNameOld from insuredlist"
cursor.execute(sql)
data = cursor.fetchall()

i=0
results=[]
for row in data:
    i=i+1
    insuredNameOld=str(row[0])
    sql2="select sum(cast(insuranceFee as decimal(18,2))) from remotedata where CreateDate>='2019-10-15 00:00:00' and policySolutionID<>'' and ExceptionStatus='' and insuredName='"+insuredNameOld+"';"
    cursor.execute(sql2)
    data2 = cursor.fetchall()
    item=[]
    if len(data2)>0:
        item.extend([insuredNameOld,str(data2[0][0])])
        print(str(i)+'，insuredNameOld='+insuredNameOld+'，sum='+str(data2[0][0]))
    results.append(item)
    

newData=pd.DataFrame(columns=['被保险人','总保费'],data=results)
newData.to_csv(str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))+'SZ.csv', encoding='utf-8', index=False)
