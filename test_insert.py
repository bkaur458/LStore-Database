#INSERT THIS RIGHT AFTER ---- # for query, args in self.queries:
# if args[0] == 92106430:
#     return self.abort()

from traceback import print_tb
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

records[92106429] = [92106429, 1, 2, 3, 4]
records[92106430] = [92106430, 5, 6, 7, 8]
q = Query(grades_table)

updated_columns_1 = [None, 100, 200, None, None]
updated_columns_2 = [None, None, None, 300, 400]
updated_columns_3 = [None, 50, 60, 70, 80]
updated_columns_4 = [None, 500, 600, 700, 800]
updated_columns_5 = [None, 5000, 6000, 7000, 8000]

query.insert(*records[92106429])
query.insert(*records[92106430])

print("\nSelecting 92106429: ")
record = query.select(92106429, 0, [1, 1, 1, 1, 1])[0]
print(record.columns)
print("\nSelecting 92106430: ")
record = query.select(92106430, 0, [1, 1, 1, 1, 1])[0]
print(record.columns)

print("\nUpdating")
query.update(92106429, 1, *updated_columns_1 )
query.update(92106430, 2, *updated_columns_2 )
query.update(92106429, 3, *updated_columns_3 )
query.update(92106429, 3, *updated_columns_4 )
query.update(92106429, 3, *updated_columns_5 )

grades_table.bufferpool.write_disk(grades_table)

val1 = []
print("\nBase Record for key 92106429: ")
for col in range(1, grades_table.num_columns+3):
    page_id = "b" + str(1) + "-" + str(1) + "-" + str(col) + "-"
    curr_page = grades_table.bufferpool.access(page_id, None)
    val = curr_page.read(0)
    val1.append(val)
print(val1)

val2 = []
print("\nBase Record for key 92106430: ")
for col in range(1, grades_table.num_columns+3):
    page_id = "b" + str(1) + "-" + str(1) + "-" + str(col) + "-"
    curr_page = grades_table.bufferpool.access(page_id, None)
    val = curr_page.read(1)
    val2.append(val)
print(val2)


