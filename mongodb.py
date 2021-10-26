import pymongo
import time
import random
import string
import numpy as np
import sys
import argparse

DEFAULT_SIZES = [100, 1000, 10000, 100000, 1000000]

UFLIST = ['RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS', 'MT', 'GO', 'DF']
SEXLIST = ['M', 'F']


def save(mod, operation, s, m, times):

	with open('mongo_{}_{}_s-{}_m-{}.txt'.format(mod, operation, s, m), 'w') as file:
			for t in times:
				file.write(str(t)+'\n')
			file.write('mean:'+str(np.mean(times))+'\n')

def randomWord(size):
   word = string.ascii_lowercase
   return ''.join(random.choice(word) for i in range(size))


def generateDate():
    dd = random.randint(1,28)
    mm = random.randint(1,12)
    aaaa = random.randint(1980, 2015)

    return f'{dd}/{mm}/{aaaa}'

def dropCollections(db):
	db['client'].drop()
	db['clientDetails'].drop()
	db['product'].drop()
	db['clientProduct'].drop()


def insert(db, size):

	
	client = db['client']
	clientDetails = db['clientDetails']
	product = db['product']
	clientProduct = db['clientProduct']


	for x in range(3):
		p = [{
			'idproduct': x,
			'name' : randomWord(10),
			'initials': randomWord(5),
			'type': randomWord(10),
			'description': randomWord(10),
			'value': random.uniform(0.0, 100.0),
		}]
		product.insert_one(*p)



	start_time_strings = time.time()
	for x in range(size):
		c = [{
			'idclient': x,
			'name' : randomWord(10),
			'sex': SEXLIST[random.randint(0,1)],
			'address': randomWord(10),
			'city': randomWord(10),
			'uf': UFLIST[random.randint(0,26)],
		}]
		client.insert_one(*c)
	end_time_strings = time.time()
	# print(f'STRINGS insert size {size} time {end_time_strings-start_time_strings}')


	for x in range(3):
		cd = [{
			'idclient': x,
			'dateBirth': generateDate(),
			'approvedLimit': random.uniform(0.0, 1000.0),
			'sinceClient': generateDate(),
			'accumuledValue': random.uniform(0.0, 1000.0),
		}]
		clientDetails.insert_one(*cd)


	start_time_numbers = time.time()
	for x in range(size):
		cp = [{
			'idclientProduct': x,
			'idclient': random.randint(0, size-1),
			'idproduct': random.randint(0, 2),
			'amount': random.randint(0, 100),
			'value': random.uniform(0.0, 100.0),
		}]
		clientProduct.insert_one(*cp)
	end_time_numbers = time.time()
	# print(f'NUMBERS insert size {size} time {end_time_numbers-start_time_numbers}')

	return end_time_strings-start_time_strings, end_time_numbers-start_time_numbers


def update(db, size):

	client = db['client']
	# clientDetails = db['clientDetails']
	# product = db['product']
	clientProduct = db['clientProduct']

	start_time_strings = time.time()
	for x in range(size):
		c = [{"idclient": x}, {"$set": {
			#'idclient': x,
			'name' : randomWord(10),
			'sex': SEXLIST[random.randint(0,1)],
			'address': randomWord(10),
			'city': randomWord(10),
			'uf': UFLIST[random.randint(0,26)],
		}}]
		client.update_one(*c)
	end_time_strings = time.time()
	# print(f'STRINGS update size {size} time {end_time_strings-start_time_strings}')

	start_time_numbers = time.time()
	for x in range(size):
		cp = [{"idclientProduct": x}, {"$set": {
			# 'idclientProduct': x,
			'idclient': random.randint(0, size-1),
			'idproduct': random.randint(0, 2),
			'amount': random.randint(0, 100),
			'value': random.uniform(0.0, 100.0),
		}}]
		clientProduct.update_one(*cp)
	end_time_numbers = time.time()
	# print(f'NUMBERS update size {size} time {end_time_numbers-start_time_numbers}')

	return end_time_strings-start_time_strings, end_time_numbers-start_time_numbers


def delete(db, size):

	client = db['client']
	# clientDetails = db['clientDetails']
	# product = db['product']
	clientProduct = db['clientProduct']


	start_time_strings = time.time()
	for x in range(size):
		c = {"idclient": x}
		client.delete_one(c)
	end_time_strings = time.time()
	# print(f'STRINGS delete size {size} time {end_time_strings-start_time_strings}')

	start_time_numbers = time.time()
	for x in range(size):
		cp = {"idclientProduct": x}
		clientProduct.delete_one(cp)
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

			dropCollections(db)

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

			dropCollections(db)

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

			dropCollections(db)

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
	port = '27017'

	conn_string = "mongodb://localhost:27017/"
	conn = pymongo.MongoClient(conn_string)

	db = conn['trabalho_final']

	dropCollections(db)

	sizes = args.list
	count = args.count

	mean(db, count, sizes)
	

if __name__ == '__main__':
	main()