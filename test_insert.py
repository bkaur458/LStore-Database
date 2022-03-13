#INSERT THIS RIGHT AFTER ---- # for query, args in self.queries:
# if args[0] == 92106430:
#     return self.abort()

from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')
# creating grades table
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1
number_of_transactions = 2
num_threads = 8

# create index on the non primary columns
try:
    grades_table.index.create_index(1)
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

keys = []
records = {}
seed(3562901)

# array of insert transactions
insert_transactions = []

for i in range(number_of_transactions):
    insert_transactions.append(Transaction())

records[92106429] = [92106429, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
records[92106430] = [92106430, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
q = Query(grades_table)
t = insert_transactions[0]
t1 = insert_transactions[1]

updated_columns = [None, 200, 100, None, None]

#queries
t.add_query(q.insert, grades_table, *records[92106429])
t.add_query(q.insert, grades_table, *records[92106430])
t1.add_query(q.update, grades_table, 92106429, *updated_columns)
t1.add_query(q.update, grades_table, 92106429, *updated_columns)
#t1.add_query(q.delete, grades_table, 92106430)
#t.add_query(q.delete, grades_table, 92106429)

insert_transactions[0].run()
insert_transactions[1].run()


# record = query.select(92106429, 0, [1, 1, 1, 1, 1])[0]
# print("\nUsing Select after transaction runs to make sure record inserted: ")
# print(record.columns)

record = query.select(92106430, 0, [1, 1, 1, 1, 1])[0]
print("\nUsing Select after transaction runs to make sure record inserted: ")
print(record.columns)