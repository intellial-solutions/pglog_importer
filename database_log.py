import sqlite3
import re
conn = sqlite3.connect('F:\Hemal Patel\logDetails')

# conn.execute('''CREATE TABLE database_log
#          (
#          date_time DATETIME,
#          ip CHAR(50),
#          host CHAR(50),
#          database CHAR(50),
#          execution_time decimal(3,3),
#          raw_query TEXT(10000));''')

import datetime
current_year = datetime.datetime.now().year

storage=[]
line_count = 0

with open("temp.txt", "r") as content:
    for line in content:
        # Pass Static value(year) if its not current year
        if line.startswith(str(current_year)):
            storage.append(line)
        else:
            line2 = storage[line_count-1] + line     
            storage[line_count-1] = line2
            line_count = line_count - 1
        line_count = line_count + 1                 
            
li = []
for line in storage:
    c1 = re.split(" UTC:",line)
    li.append(c1[0])
    c2 = re.split(":",c1[1],maxsplit=1)
    li.append(c2[0])
    c3 = re.split("@",c2[1],maxsplit=1)
    li.append(c3[0])
    c4 = re.split(":LOG:  duration: ",c3[1],maxsplit=1)
    li.append(c4[0])
    c5 = re.split(" ms  statement: ",c4[1],maxsplit=1)
    li.append(c5[0])
    li.append(c5[1])
    
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO database_log ( date_time, ip, host,database, execution_time, raw_query) VALUES (?,?,?,?,?,?) ",(li,))    
    li.clear()   

conn.commit()
cursor.close()
print("Data has been successfully uploaded!")