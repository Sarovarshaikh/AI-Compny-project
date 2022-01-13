import pandas as pd
import pymysql as p

df = pd.read_csv("movies.csv")

df.dropna(inplace=True)#delete all the missing data from dataset

#print(df.head())
table = []

#for rec in df.itertuples():
#    table.append(rec)

for i, a,b,c,d,e,f in df.itertuples():
    table.append([i,a,b,c,d,e,f])


#==================connect with xmapp

def getconnect():
    return p.connect(host="localhost", user="root", password="", database="bootcamp")


def insertrec(t):
    db = getconnect()
    cr = db.cursor()

    sql= "insert into mydata values(%s,%s,%s,%s,%s,%s)"
    cr.execute(sql,t)
    db.commit()
    db.close()




def senddata():
    
    for data in range(len(table)):
        insertrec(tuple(table[data]))
    print("data transfered successfully....!")

