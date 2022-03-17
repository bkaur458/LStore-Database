from lib2to3.pytree import Base
from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 10000
number_of_aggregates = 100
number_of_updates = 5

seed(3562901)
for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

#Check for the base record, i.e. -5 version
keys = sorted(list(records.keys()))
for key in keys:
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -5)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error and key == 92106429:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
print("Select for version -5 finished")

#Check for -6 version, still base record
keys = sorted(list(records.keys()))
for key in keys:
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -6)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error and key == 92106429:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
print("Select for version -6 finished")

'''
UPDATE              - VERSION
Base                -5 
1st update          -4
2nd update          -3 
3rd update          -2 
4th update          -1 
5th update           0    
'''

############ -4 version
updated_records = {}
keys = sorted(list(records.keys()))
for _ in range(number_of_updates):
    for key in keys:
        updated_records[key] = records[key].copy()
        for j in range(2, grades_table.num_columns):
            value = randint(0, 20)
            updated_records[key][j] = value

# Will check for version -4 
keys = sorted(list(records.keys()))
for key in keys:
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -4)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != updated_records[key][i]:
            error = True
    if error and key == 92106429:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
print("Select for version -4 finished")

############ -3 version
updated_records = {}
keys = sorted(list(records.keys()))
for _ in range(number_of_updates):
    for key in keys:
        updated_records[key] = records[key].copy()
        for j in range(2, grades_table.num_columns):
            value = randint(0, 20)
            updated_records[key][j] = value

# Will check for version -3
keys = sorted(list(records.keys()))
for key in keys:
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -3)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != updated_records[key][i]:
            error = True
    if error and key == 92106429:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
print("Select for version -3 finished")

############ -2 version
updated_records = {}
keys = sorted(list(records.keys()))
for _ in range(number_of_updates):
    for key in keys:
        updated_records[key] = records[key].copy()
        for j in range(2, grades_table.num_columns):
            value = randint(0, 20)
            updated_records[key][j] = value

# Will check for version -2
keys = sorted(list(records.keys()))
for key in keys:
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != updated_records[key][i]:
            error = True
    if error and key == 92106429:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
print("Select for version -2 finished")

############ -1 version
updated_records = {}
keys = sorted(list(records.keys()))
for _ in range(number_of_updates):
    for key in keys:
        updated_records[key] = records[key].copy()
        for j in range(2, grades_table.num_columns):
            value = randint(0, 20)
            updated_records[key][j] = value

# Will check for version -1
keys = sorted(list(records.keys()))
for key in keys:
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != updated_records[key][i]:
            error = True
    if error and key == 92106429:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
print("Select for version -1 finished")

############ 0 version
updated_records = {}
keys = sorted(list(records.keys()))
for _ in range(number_of_updates):
    for key in keys:
        updated_records[key] = records[key].copy()
        for j in range(2, grades_table.num_columns):
            value = randint(0, 20)
            updated_records[key][j] = value

# Will check for version 0
keys = sorted(list(records.keys()))
for key in keys:
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != updated_records[key][i]:
            error = True
    if error and key == 92106429:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
print("Select for version 0 finished")

# for i in range(0, number_of_aggregates):
#     r = sorted(sample(range(0, len(keys)), 2))
#     column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
#     result = query.sum_version(keys[r[0]], keys[r[1]], 0, -1)
#     if column_sum != result:
#         print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
# print("Aggregate version -1 finished")

# for i in range(0, number_of_aggregates):
#     r = sorted(sample(range(0, len(keys)), 2))
#     column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
#     result = query.sum_version(keys[r[0]], keys[r[1]], 0, -2)
#     if column_sum != result:
#         print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
# print("Aggregate version -2 finished")


# for i in range(0, number_of_aggregates):
#     r = sorted(sample(range(0, len(keys)), 2))
#     updated_column_sum = sum(map(lambda x: updated_records[x][0] if x in updated_records else 0, keys[r[0]: r[1] + 1]))
#     updated_result = query.sum_version(keys[r[0]], keys[r[1]], 0, 0)
#     if updated_column_sum != updated_result:
#         print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', updated_result, ', correct: ', updated_column_sum)
# print("Aggregate version 0 finished")

deleted_keys = sample(keys, 100)
for key in deleted_keys:
    query.delete(key)
    records.pop(key, None)

db.close()