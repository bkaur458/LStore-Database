from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)


# #TEST 1 - TESTING ONLY WHEN BASE RECORD NEVER UPDATED
# records = {}
# records[1] = [1, 2, 3, 4, 5]
# query.insert(*records[1])
# record = query.select_version(1, 0, [1,1,1,1,1], 4)[0].columns
# print(record)

# #TEST 2 - TESTING VERSIONS WITHOUT MERGE
# records = {}
# records[1] = [1, 2, 3, 4, 5]
# query.insert(*records[1])

# updated_columns = [None, 200, 300, 400, 500]
# record = query.update(1, *updated_columns)

# record = query.select_version(1, 0, [1,1,1,1,1], 0)[0].columns
# print(record)


