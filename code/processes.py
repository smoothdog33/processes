from datetime import datetime
import os 
import psutil
import self
import json
import pandas as pd
import sqlalchemy
import psycopg2
import psycopg2.extras as extras

pgconn = psycopg2.connect(
    host= '192.168.86.49',
    user = 'dev_test1',
    password = 'ayan',
    database = 'testing')
pgcursor = pgconn.cursor()



def process(output_file_name):
    pid_list = psutil.pids()
    for x in pid_list:
        ##print(x)
        times =  psutil.cpu_times(x)
        virtual_memory = psutil.virtual_memory()
        disk_partitions = psutil.disk_partitions(x)
        users = psutil.users()
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        #print("Current Time =", current_time)
        #print (times)
        #print (virtual_memory)
       # print (disk_partitions)
        #print (((disk_partitions[0])))
       # print(type(str(disk_partitions)))
        disk_partitions_str = str(disk_partitions).replace("sdiskpart","")
       # print(disk_partitions_dict.replace("=","\":").replace("(","{\"").replace(")","}").replace(", ", ", \""))
        disk_partitions_list = disk_partitions_str.replace("=","\":").replace("[(","{\"").replace(")]","}").replace(", ", ", \"")
        disk_partitions_dict = eval(disk_partitions_list)
        

        process_info = {"times":times,"virtual_memory":virtual_memory,"disk_partitions_dict":disk_partitions_dict,"current_time":current_time,"users":users}
        #process_info = {"times":times,"virtual_memory":virtual_memory,"users":users}
        #key_list = list(disk_partitions.keys())
        #print (key_list)
        for x in disk_partitions_dict[0]: 
            stri = "disk_partitions" 
            #print(x)
            #print(disk_partitions[0].index(x))
            #locals()["_".join([stri, str(x)])] = disk_partitions[x]
            #print*(disk_partitions_1)
        #process_info = {"dtisk_partiions":disk_partitions,"disk_partitions":times,"virtual_memory":virtual_memory,"current_time":current_time,"users":users}

            with open(output_file_name, 'a') as file_name:
                json.dump(process_info,file_name)
                file_name.write("\n")
process('firstrun12.json')

def json_inserter(file_name,table_name):
        pgconn = psycopg2.connect(
        host= '192.168.86.49',
        user = "dev_test1",
        password = 'ayan',
        database = 'testing')
        pgcursor = pgconn.cursor()
       
        df = pd.read_json (file_name,lines=True)
        df['disk_partitions_dict'] = list(map(lambda x: json.dumps(x), df['disk_partitions_dict']))
        #df.shape
        #display(df.dtypes)
        from sqlalchemy import create_engine
        engine = create_engine('postgresql+psycopg2://dev_test1:ayan@ayan-Virtual-Machine/testing')
        #print(engine)
        df.to_sql(table_name, engine, if_exists = 'append', index=False)
        
        #df[['disk_partitions', 'users']] = df[['users', 'users']].astype(float)
json_inserter('firstrun12.json','processes2')


def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()

pgconn = psycopg2.connect(
        host= '192.168.86.49',
        user = "dev_test1",
        password = 'ayan',
        database = 'testing')

#df = pd.read_json ('firstrun8.json',lines=True)
#disk_partitions = df[['disk_partitions']] 
#print (disk_partitions)
#df[['disk_partitions', 'users']] = df[['disk_partitions', 'users']].astype(object)
#df['a','b']= pd.DataFrame(df.disk_partitions.tolist().tolist(),index= df.index)

#display(df.dtypes)

#execute_values(pgconn, df, 'processes8')

#
# 
# "INSERT INTO processes2 (times, virtual_memory, Currnt_Time, users, disk_partitions)"
#values('" +  times  + ",'" + virtual_memory + ",'" +  Current_Time  + ",'"  +  users  + ",'"  + disk_partitions + ",')" )