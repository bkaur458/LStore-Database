from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 3 grades
db = Database()
scores_table = db.create_table('SAT_Scores', 4, 0)
query = Query(scores_table)
keys = []

# Purposely didn't have a for loop for now
query.insert(101, 1, 2, 3)
keys.append(101)
query.insert(102, 4, 5, 6)
keys.append(102)
query.insert(103, 7, 8, 9)
keys.append(103)
query.insert(104, 10, 11, 12)
keys.append(104)
query.insert(105, 13, 14, 15)
keys.append(105)

print ("\nAll Records before update: ")
# Testing Select
for key in keys:
    ind_out = query.select(key,0 , [1, 1, 1, 1])
    print (ind_out[0].columns)

update_cols = [
    [None, 111, None, None],
    [None, None, 111, None],
    [None, None, None, 111],
]

key_selected = choice(keys)
print ("\nUpdating record with key: " + str(key_selected))
# Updating a record
query.update(key_selected, *(choice(update_cols)))

print ("\nAll Records after update: ")
# Testing Select
for key in keys:
    ind_out = query.select(key,0 , [1, 1, 1, 1])
    print (ind_out[0].columns)

# DELETE
key_selected = choice(keys)
print ("\nDeleting record with key: " + str(key_selected))

rid = scores_table.key_dict[key_selected]
[range_no, base_page_no, slot_no] = scores_table.page_directory[rid]

indirection_value = scores_table.page_ranges_dictionary[range_no].base_pages[base_page_no].pages[1].read(slot_no)

print("\nIndirection value for key " + str(key_selected) + " before delete: " + str(indirection_value))
query.delete(key_selected)
indirection_value = scores_table.page_ranges_dictionary[range_no].base_pages[base_page_no].pages[1].read(slot_no)
print("Indirection value for key " + str(key_selected) + " after delete: " + str(indirection_value) + "\n")
