import psycopg2
import random
import time
import string
import numpy as np
import sys
import argparse

DEFAULT_SIZES = [100, 1000, 10000, 100000, 1000000]

UFLIST = ['RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS', 'MT', 'GO', 'DF']
SEXLIST = ['M', 'F']

def save(mod, operation, s, m, times):

    with open('postgres_{}_{}_s-{}_m-{}.txt'.format(mod, operation, s, m), 'w') as file:
            for t in times:
                file.write(str(t)+'\n')
            file.write('mean:'+str(np.mean(times))+'\n')

def generateDate():
    dd = random.randint(1,28)
    mm = random.randint(1,12)
    aaaa = random.randint(1980, 2015)

    return f'{dd}/{mm}/{aaaa}'

def dropTables(db):
    db.execute('DROP TABLE clientdetails;')
    db.execute('DROP TABLE clientproduct;')
    db.execute('DROP TABLE client;')
    db.execute('DROP TABLE product;')

def createTable(db):
    sql_t1 = "CREATE TABLE client(idclient integer not null, name varchar(10), sex varchar(1), address varchar(10), city varchar(10), uf varchar(2), primary key(idclient))"
    db.execute(sql_t1)
    sql_t2 = "CREATE TABLE clientDetails(idclient integer not null, dateBirth DATE, approvedLimit REAL, sinceClient DATE, accumuledValue REAL, primary key(idclient), constraint fk_client foreign key (idclient) references client(idclient))"
    db.execute(sql_t2)
    sql_t3 = "CREATE TABLE product(idproduct integer not null, name varchar (10), initials varchar(5), type varchar(10), description varchar(10), value REAL, primary key(idproduct))"
    db.execute(sql_t3)
    sql_t4 = "CREATE TABLE clientProduct(idclientProduct integer not null, idclient integer not null, idproduct integer not null, amount integer not null, value REAL, primary key(idclientProduct), constraint fk_client foreign key (idclient) references client(idclient), constraint fk_product foreign key (idproduct) references product(idproduct))"
    db.execute(sql_t4)

def randomWord(size):
   word = string.ascii_lowercase
   return ''.join(random.choice(word) for i in range(size))


def insert(db, size):

    for x in range(3):
        sql_t1 = "insert into product values ("+ str(x) +", '"+ randomWord(10) +"', '"+ randomWord(5) +"', '"+ randomWord(10) +"', '"+ randomWord(10) +"', '"+ str(random.uniform(0.0, 100.0))+"')"
        db.execute(sql_t1)    

    start_time_strings = time.time()
    for x in range(size):
        sql_t2 = "insert into client values ("+ str(x) +", '"+ randomWord(10) +"', '"+ SEXLIST[random.randint(0,1)] +"', '"+ randomWord(10) +"', '"+ randomWord(10) +"', '"+ UFLIST[random.randint(0,26)]+"')"
        db.execute(sql_t2)
    end_time_strings = time.time()
    # print(f'STRINGS insert size {size} time {end_time_strings-start_time_strings}')
    
    for x in range(3):
        sql_t3 = "insert into clientDetails values ("+ str(x) +", '"+ generateDate() +"', '"+ str(random.uniform(0.0, 1000.0))+"', '"+ generateDate() +"', '"+ str(random.uniform(0.0, 1000.0))+"')"
        db.execute(sql_t3)
    
    start_time_numbers = time.time()
    for x in range(size):
        sql_t4 = "insert into clientProduct values ("+ str(x) +", "+ str(random.randint(0,size-1)) +", " + str(random.randint(0, 2)) +", "+ str(random.randint(0, 100)) +", "+ str(random.uniform(0.0, 100.0))+")"
        db.execute(sql_t4)
    end_time_numbers = time.time()
    # print(f'NUMBERS insert size {size} time {end_time_numbers-start_time_numbers}')

    return end_time_strings-start_time_strings, end_time_numbers-start_time_numbers

def update(db, size):

    start_time_strings = time.time()
    for x in range(size):
        sql_t2 = "update client set name='"+ randomWord(10) +"', sex='"+ SEXLIST[random.randint(0,1)] +"', address='"+ randomWord(10) +"', city='"+ randomWord(10) +"', uf='"+ UFLIST[random.randint(0,26)]+"' WHERE idclient=" + str(x)
        db.execute(sql_t2)
    end_time_strings = time.time()
    # print(f'STRINGS update size {size} time {end_time_strings-start_time_strings}')
    

    start_time_numbers = time.time()
    for x in range(size):
        sql_t4 = "update clientProduct set idclient="+ str(random.randint(0,size-1)) +", idproduct=" + str(random.randint(0, 2)) +", amount="+ str(random.randint(0, 100)) +", value="+ str(random.uniform(0.0, 100.0))+" WHERE idclientProduct=" + str(x)
        db.execute(sql_t4)
    end_time_numbers = time.time()
    # print(f'NUMBERS update size {size} time {end_time_numbers-start_time_numbers}')

    return end_time_strings-start_time_strings, end_time_numbers-start_time_numbers

def delete(db, size):

    db.execute('DROP TABLE clientdetails;') # tem q excluir antes, pq eh chave estrangeira
    db.execute('DROP TABLE clientproduct;') # tem q excluir antes, pq eh chave estrangeira
    start_time_strings = time.time()
    for x in range(size):
        sql_t2 = "delete from client WHERE idclient=" + str(x)
        db.execute(sql_t2)
    end_time_strings = time.time()
    # print(f'STRINGS delete size {size} time {end_time_strings-start_time_strings}')
    
    db.execute('DROP TABLE client;') # limpar o banco, sem nenhuma tabela 
    db.execute('DROP TABLE product;') # limpar o banco, sem nenhuma tabela
        
    createTable(db) # cria banco novamente

    insert(db, size) # insere tudo de novo para poder deletar clientProduct

    start_time_numbers = time.time()
    for x in range(size):
        sql_t4 = "delete from clientProduct WHERE idclientProduct=" + str(x)
        db.execute(sql_t4)
    end_time_numbers = time.time()
    # print(f'NUMBERS delete size {size} time {end_time_numbers-start_time_numbers}')

    return end_time_strings-start_time_strings, end_time_numbers-start_time_numbers


def mean(db, iterations, sizes):
        
    for s in sizes:
        times_strings = []
        times_numbers = []
        for i in range(iterations):
            ts, tn = insert(db, s)
            times_strings.append(ts)    
            times_numbers.append(tn)

            dropTables(db)
            createTable(db)

        times_strings = np.array(times_strings)
        times_numbers = np.array(times_numbers) 
        
        save('strings', 'insert', s, iterations, times_strings)
        save('numbers', 'insert', s, iterations, times_numbers)

    for s in sizes:
        times_strings = []
        times_numbers = []
        for i in range(iterations):
            insert(db, s)
            ts, tn = update(db, s)
            times_strings.append(ts)    
            times_numbers.append(tn)

            dropTables(db)
            createTable(db)           

        times_strings = np.array(times_strings)
        times_numbers = np.array(times_numbers) 
        
        save('strings', 'update', s, iterations, times_strings)
        save('numbers', 'update', s, iterations, times_numbers)
    

    for s in sizes:
        times_strings = []
        times_numbers = []
        for i in range(iterations):
            insert(db, s)
            ts, tn = delete(db, s)
            times_strings.append(ts)    
            times_numbers.append(tn)

            dropTables(db)
            createTable(db) 

        times_strings = np.array(times_strings)
        times_numbers = np.array(times_numbers) 
        
        save('strings', 'delete', s, iterations, times_strings)
        save('numbers', 'delete', s, iterations, times_numbers)
        

def main():

    parser = argparse.ArgumentParser(description='testes')
    parser.add_argument("--count", "-c", help="number of executions", type=int)
    parser.add_argument("--list", "-l", nargs='+', help="list sizes", default=DEFAULT_SIZES, type=int)
    args = parser.parse_args()

    host = "localhost"
    dbname = "trabalho_final"
    user = "postgres"
    password = "postgres"
    sslmode = "require"

    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string)
    
    db = conn.cursor()  
    
    dropTables(db)
    createTable(db)
    
    sizes = args.list
    count = args.count

    mean(db, count, sizes)
    
    db.close()
    conn.commit()
    conn.close()

if __name__ == '__main__':

	main()
