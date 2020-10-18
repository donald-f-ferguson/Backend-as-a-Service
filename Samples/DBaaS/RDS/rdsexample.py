import pymysql
import os
import json

#
# Do not store sensitive information in files that you plan to put in GitHub.
# I am getting from the environment.
#
# This also lets you change from instance to instance without modifying the code.
#
#
db_user = os.environ.get('DB_USER')
db_pw = os.environ.get('DB_PW')
db_host = os.environ.get('DB_HOST')

conn = pymysql.connect(
    host=db_host,
    user=db_user,
    password=db_pw,
    cursorclass=pymysql.cursors.DictCursor)


#
# You can use like any old RDB, it is just "as-a-Service" in the cloud.
# Do not need to buy machine, install SW, purchase licenses, backup, monitor, ... ...
#
cur = conn.cursor()
res = cur.execute("select * from classicmodels.customers limit 10;")
res = cur.fetchall()

print("Ten customers = ")
print(json.dumps(res, indent=3, default=str))

