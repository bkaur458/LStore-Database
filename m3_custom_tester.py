from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 2
number_of_transactions = 2
number_of_operations_per_record = 1
num_threads = 2

keys = []
records = {}
seed(3562901)

records[92106429] = [92106429, 2,3,4,5]
keys.append(92106429)
records[92106430] = [92106430, 6,7,8,9]
keys.append(92106430)

transaction_workers = []
transactions = []

for i in range(number_of_transactions):
    transactions.append(Transaction())

for i in range(num_threads):
    transaction_workers.append(TransactionWorker())

# x update on every column
updated_columns = [None, 200, None, None, None]
transactions[0].add_query(query.select, grades_table, keys[0], 0, [1, 1, 1, 1, 1])
transactions[0].add_query(query.update, grades_table, keys[0], *updated_columns)
updated_columns = [None, 2000, None, None, None]
transactions[0].add_query(query.select, grades_table, keys[0], 0, [1, 1, 1, 1, 1])
transactions[0].add_query(query.update, grades_table, keys[0], *updated_columns)

# x update on every column
updated_columns = [None, 6000, None, None, None]
transactions[1].add_query(query.select, grades_table, keys[1], 0, [1, 1, 1, 1, 1])
transactions[1].add_query(query.update, grades_table, keys[1], *updated_columns)
print("Update finished")

# add trasactions to transaction workers  
transaction_workers[0].add_transaction(transactions[0])
transaction_workers[1].add_transaction(transactions[1])

# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()

for key in keys:
    query = Query(grades_table)
    result = query.select(key, 0, [1, 1, 1, 1, 1])[0].columns
    print('select', key, ':', result)

db.close()