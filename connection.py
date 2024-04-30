import sqlite3
def databaseconnection(q):
    conectionstring=sqlite3.connect('D:\ontropiProject\CDNProject\python\Data Engineer_ETL Assignment.db')

    connection_obj=conectionstring.cursor()

    data=connection_obj.execute(q)

    return data.fetchall()


#SELECT * from customers
# data=databaseconnection()


# for i in data.fetchall():
#     print(i)