#package section :

import numpy as np
import pandas as pd
import os, sys


import psycopg2

''' 
 Connection class is mainly use to have a secure connection to DB.

'''
def connection():
    try:
        conn=psycopg2.connect("host=localhost dbname=dka3830 user=dka3830")

        return conn
    except:
        print('connection time was out of range')


def extract_table_in_list(con = connection(), shema = 'public'):
    conn = connection()
    cur = conn.cursor()
    cur.execute("SELECT table_name "
                "FROM information_schema.tables"
                " WHERE table_schema='{s}'"
                "   AND table_type='BASE TABLE';".format(s=str(shema)))
    table_list = []
    for row in cur:
        table_list.append(row[0])
    return table_list

def generate_SQL(shema,table_list):
    query_list = []
    for table_name in table_list:
        sql= """select dgag.dauid, count(inside.geom) as num_{t_n} from {s}."{name}" as inside, dgag."DA_CAN_2016" as dgag where st_within(inside.geom,dgag.geom) group by dgag.dauid;""".format(t_n = table_name , s = shema,name =table_name) ;
        query_list.append(sql)
    return query_list


def execute_query(query,num, con = connection()):
    import pandas as pd
    df = pd.read_sql(query, con)
    df.to_csv("/home/dka3830/Documents/proj/output/result_{n}.csv".format(n=num), sep=',')
    con.close()
    return 0





if __name__ == '__main__':
    print('-------------------------------------------')
    print('             Query output                 ')
    print('-------------------------------------------')

## Write SQL query

    table_list = extract_table_in_list(shema='here')

    query_list = generate_SQL('here', table_list)

    # table to be merge to (AD_ID).
    query1 = 'select "dauid" from dgag."DA_CAN_2016"'
    data0 = execute_query(query1,999, connection())

    try:

        i=0
        for query in query_list:
            if i <=23:
                i += 1
            else:
                data = execute_query(query,i,connection())
                i += 1
                print("The table to be merged next is number : {n}".format(n=i))
    except:
        print(" A file wasn't save ")
    # save result to a CSV
    #data0.to_csv("/home/dka3830/Documents/proj/output/result_01.csv", spe=',')
    print('\n')
    print('-------------------------------------------')
    print('              End Message                  ')
    print('-------------------------------------------')
    print('\n\n')
