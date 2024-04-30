from connection import databaseconnection
import pandas as pds
import csv
from sqlalchemy import create_engine
import os
class Fetchdata:

    def __init__(self,query):
        self.query=query
        self.database_path = os.path.join(os.getcwd(), 'Data Engineer_ETL Assignment1.db')
        self.engine = create_engine('sqlite:///' + self.database_path)
        #self.engine = create_engine('D:\ontropiProject\CDNProject\python\Data Engineer_ETL Assignment.db')

    def getdata(self):
        
        try:
            data=databaseconnection(self.query) #using noraml sql connection
            return data
        except Exception as e:
            print('You have some error',e)
            return None
        
    

    def Import_CVS(self):
         data=self.getdata()
         if data is not None:
            with open('data.csv','w',newline='') as file:
                csv_writer=csv.writer(file) #it will print in seperate column with each value
                #csv_writer=csv.writer(file,delimiter=';') #it will print in single column with ; seperate..
                csv_writer.writerow(['Age','Item name','TotalQuantity'])
                csv_writer.writerows(data)
         else:
             print('Data not Found')
         

    def PandasCVS(self):
         data=pds.read_sql_query(self.query,con=self.engine)
         if data is not None:
            #data.to_csv('pandasCsvdata.csv',sep=';',index=False) #it will print in single  column with ; seperater
            data.to_csv('pandasCsvdata.csv',index=False)  #it will print in seperate column 
         else:
             print('Data not Found')
         
        

q='''
select c.age as Age,i.item_name as Item,coalesce(sum(o.quantity),0) as TotalQuantity from sales as s 
JOIN customers as c on s.customer_id=c.customer_id
JOIN orders as o on s.sales_id=o.sales_id
JOIN items as i on o.item_id=i.item_id
where i.item_name in ('x','y','z') GROUP by c.age,i.item_name
ORDER by c.age,i.item_name;
'''
obj=Fetchdata(q)
obj.Import_CVS()
obj.PandasCVS()







