# from random import randint
# from re import S
# from lstore.table import Table
# from time import process_time
# from lstore.query import Query
# from random import choice, randint, sample, seed

# grades_table = Table('Grades', 5, 0)
# curr_range = grades_table.page_ranges_dictionary[grades_table.num_of_ranges]
# #max capacity set to 1 for testing
# query = Query(grades_table)
# rec= {}
# for i in range(0,17):
#     rec[i] = [i,2,4,5,6]
# for i in range(0,17):
#     query.insert(*rec[i])
# rec[99] = [99,2,4,5,6]
# rec[95] = [95,2,4,5,6]
# query.update(5, *rec[99])
# print(query.update(99, *rec[95]))
# print(grades_table.page_ranges_dictionary[1].base_pages[1].pages[3].read(15))
# record = query.select(15, 0, [1, 1, 1, 1, 1])[0]
# print(record.columns)
# print(grades_table.page_ranges_dictionary[1].base_pages[6].pages[1].read(0))
# #query.delete(95)
# print(grades_table.page_ranges_dictionary[1].base_pages[6].pages[1].read(0))
# print(query.sum(0, 96, 1))

# '''
# print(test_page.write(10))
# print(test_page.write(20))
# print(test_page.write(30))
# print(test_page.write(40))
# #Testing Over Capacity
# print(test_page.write(50))

# print(test_page.read(0))
# print(test_page.read(1))
# print(test_page.read(2))
# print(test_page.read(3))

# ## curr_range.add_base_page() // has to be internal method that needs to be called by page range
# test_page = curr_range.base_pages[2]
# insert_time_0 = process_time()
# for i in range(1,501):
#     test_page.write(i)
# insert_time_1 = process_time()

# print("Time needed to insert 500 records in base page 2")
# print(insert_time_1 - insert_time_0)

# print(test_page.read(499))

# print(test_page.read(0))
# '''
# #making sure both pages exist 
# #main page no longer exists

# '''

# # -------------------- Tail Pages --------------------

# #print(curr_range.tail_pages[1]) Will Give Key Error
# '''

list = [1,2,3]
print(len(list))