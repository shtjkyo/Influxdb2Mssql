# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 13:33:53 2017

@author: finere
"""
import sys
import pymssql
from influxdb import InfluxDBClient

reload(sys)
sys.setdefaultencoding('utf8')
conn = pymssql.connect(server="localhost",port="3723",user="admin",password="admin",database="historianstorage",charset="UTF-8")
client = InfluxDBClient(host="localhost", port="8086", username='admin', password='admin', database='demo')
cur = conn.cursor()
# 查询操作
if not cur:  
    raise Exception('数据库连接失败！')  
sSQL = "select Tagname,max(ValueFloat/10) Value,max(TimeStamp) Time \
    from [HistorianStorage].[TLG].[VTagValue],[HistorianStorage].[IS].[VTagBrowsing] \
    where ( \
           [HistorianStorage].[TLG].[VTagValue].[TagUID]=[HistorianStorage].[IS].[VTagBrowsing].[TagUID] and \
           [HistorianStorage].[TLG].[VTagValue].[TagUID] in \
           ('8182E362-B5C1-468F-82E4-A7F68A4B5138',\
            '7BC5BDD8-75B6-4CC8-8815-DC4495352F22',\
            'E4CFB84B-4D3E-4A58-9909-979BADDCC6B5',\
            '8423E786-6759-46D0-8193-99AE7A75E3AC',\
            '98DA5472-3C80-47B3-ABAB-E09A92DD73B9',\
            '7E7DDEEE-7E4C-4F22-BE41-526FF2A24D62',\
            '09DACEE4-E51D-4D89-B12E-021F43333438',\
            'AE156750-D3C4-42A3-AED5-0E7ED72EA1AB',\
            '312FBE32-B8B2-46CC-97DF-0CC8054F1146',\
            '0C607E3B-0A7A-450A-A220-21C3D7EF0988',\
            '75E0703D-4BC0-4752-A82D-75A0EFEDD03D',\
            '48160BF6-BA0B-4A71-A1C8-9844C897C771',\
            '1094A4C3-C967-42D5-9948-864F5ADE5CF7',\
            '30AC286C-D00C-485C-A571-76CC0EF99B8E',\
            '69D28EE6-B361-4A0B-A8F9-1DA33D99B597',\
            '4134339F-BB46-46E9-9C56-328D080998D1',\
            '263F8C75-D577-4A91-BEA8-8D04EA52F5CA',\
            '852B7B9C-9267-4305-9A5F-76D0D00576FE',\
            'E97A0CDA-7CF8-4B74-B3E8-27A60EA0B1E6',\
            '4E969D8B-6109-41E5-B701-3A8F4B94CBDA',\
            '679298B9-F2C9-41A4-AA76-B015E9D9993C',\
            '4489E39F-0D9E-469C-82BA-A14BCF0F7399',\
            'CDE7A6DA-DB60-4A79-BBD5-A79C31CA944C',\
            '0F6328A2-89D9-47A8-A38C-579DF98CC81E',\
            '594AC085-1485-4B94-A57E-8A9EC23A504F',\
            '50613102-3457-4F1A-8819-C1DE6624782C',\
            'EB2B1B89-4B0F-4782-B634-DB3506D68511',\
            'BD46F438-BF3B-4850-88F0-80AE5C7B3082',\
            '5748D720-6C8C-4606-BA7D-83CA01714C88',\
            'F3822995-6980-4F57-945C-013DE3EBFD5D',\
            '74CAD8F3-5DB2-4F46-81BB-C4C823B5F48A',\
            'B75835B9-43CD-430A-BB5B-0C464F5DEEB2'\
            ) \
            ) \
            group by Tagname "
#1.执行sql，获取所有数据 
cur.execute(sSQL)  
rows=cur.fetchall()  
conn.close()  


for row in rows:
        json_body = [
                {
                 "measurement": "EastMeter",
                 "tags": {
                       "Tagname": "EastMeter",            
                },
                  #"time": row[2],
                  "fields": {
                         row[0]: row[1]
                }
                }
                ]
        client.write_points(json_body)  # 写入数据


