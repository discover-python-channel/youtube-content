# -*- coding: utf-8 -*-

from mysql.connector import pooling
from mysql.connector import connect
import time 
import random





#### create connection pool #########
def create_connection_pool():
    db_config = {
        'host' : 'db',
        'user' : 'test',
        'password' : 'password',
        'database' : 'test',
        'port' : 3306,
    }
    cnxpool = pooling.MySQLConnectionPool(pool_name = "example_pool", pool_size = 20, autocommit=True,  **db_config)

    return cnxpool

#### create connection 
def create_connection():
    db_config = {
        'host' : 'db',
        'user' : 'test',
        'password' : 'password',
        'database' : 'test',
        'port' : 3306,
    }
    cnx = connect(autocommit=True, **db_config)
    return cnx



#### create empty table #########
def create_empty_table(connection):
        create_table =  'CREATE TABLE Persons (PersonID int, FirstName varchar(255), Age int)'
        cursor = connection.cursor()
        cursor.execute(create_table)
        cursor.close()
        connection.close()



#### define a function to load data into mysql 
def load_fake_data(connection, names_list):
    person_id = random.randint(0,1000000)
    first_name = names_list[random.randint(0,len(names_list) - 1)]
    age = random.randint(1,80)
    sql_statement = f"insert into Persons (PersonID, FirstName, Age) values ( { person_id } , '{ first_name }', { age } )"    
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    cursor.close()
    connection.close()

    return


### create connection pool to load database
def main():
    results = []
    names_list = ['john', 'dave', 'johnas','steven', 'april', 'barbara', 'ted' , 'json']

    
    ## connection pooling 
    cnx = create_connection_pool()
    create_empty_table(cnx.get_connection())


    ### individual connections
    #create_empty_table(create_connection())

    for i in range(0, 1000):

        start_time = time.time()
        ##### run with connection pool 
        load_fake_data(cnx.get_connection(), names_list)

        ### run with single connection
        #load_fake_data(create_connection(), names_list)
        
        results.append(time.time() - start_time)
    
    print(f'Average processing time {sum(results) / len(results)}')

if __name__ == '__main__':
    main()