from ast import arg

from torch import stack
from lstore.query import Query
from lstore.table import Table, Record
from lstore.index import Index
import time
from os.path import exists

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        #assigns a unique id based on time in milliseconds
        self.transaction_id = int(round(time.time() * 1000))
        #for now, mainly used to access key_dict and page_dictionary of a table
        self.table = None
        self.stack = []
        #Need a query object to use query functions
        self.query_obj = None
        pass

    '''
    Stack logic for now:
    If a query runs successfully:
        add to log and then append to the stack
    If a query fails:
        call abort
    If a commit happens:
        update bufferpool pages
        mark them dirty
        write to disk
        mark them non-dirty
        pop all queries from the stack
    If an abort happens:
        Go thru each query in the stack, and undo/ redo as needed
        pop them all
    '''

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, table, query, *args):
        if self.table == None:
            self.table = table
        if self.query_obj == None:
            self.query_obj = Query(self.table)
        self.queries.append((query, args))
        # use grades_table for aborting

    '''
    Entry in a Log looks like this for now:
    4       bytes - serial #
    32      bytes - xcat id
    4       bytes - action (1 - insert, 2- update, 3- delete)
    4       bytes - RID
    (N+2)*8 bytes - old values for N+2 columns 
    (N+2)*8 bytes - new values for N+2 columns

    Also added a ctr attribute in table class to give serial numbers to each entry
    '''

    '''
    Entry in a stack is a list for each query and looks like this for now:
    Index     Value
    0       - Xcat id
    1       - Action - (1 - insert, 2- update, 3- delete)
    2       - RID 
    3       - List of old values
    4       - List of new values
    '''

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            query_entry_for_stack= []
            fin = open("log.txt", "r+b")

            #1. Adding ctr (serial #) to log:
            #   Increment ctr first, then insert to log (ctr is initialized as 0 in the table class
            #   so the first entry in log will have a ctr of 1)
            self.table.ctr += 1                        
            fin.write(bytearray((self.table.ctr).to_bytes(4, 'big', signed=True)))

            #2. Adding transaction id to log
            fin.write(bytearray((self.transaction_id).to_bytes(32, 'big', signed=True)))
                # Also add to stack
            query_entry_for_stack.append(self.transaction_id)

            #3. Adding action type
            if("insert" in str(query)):
                fin.write(bytearray((1).to_bytes(4, 'big', signed=True)))
                query_entry_for_stack.append(1)
            elif("update" in str(query)):
                fin.write(bytearray((2).to_bytes(4, 'big', signed=True)))
                query_entry_for_stack.append(2)
            elif("delete" in str(query)):
                fin.write(bytearray((3).to_bytes(4, 'big', signed=True)))
                query_entry_for_stack.append(3)

            #4. Adding RID id to log:
            #   Get primary key first
            primary_key = args[0][0] 
            #   get rid using key_dict
            rid = self.table.key_dict(primary_key)
            #   write rid to the log    
            fin.write(bytearray((rid).to_bytes(4, 'big', signed=True)))
            #   Also add to stack
            query_entry_for_stack.append(rid)
            
            #5. Adding old values to the log
            old_values = []
            #   Since insert doesn't have any old values, we can insert 0 maybe?
            if("insert" in str(query)):
                for col_no in range(1, self.table.num_columns+3):
                    old_values.append(0)
                    fin.write(bytearray((0).to_bytes(8, 'big', signed=True)))
            #   For update and delete, find old values using bufferpool access
            else:
                #TODO: haven't included reading from indirection column, just reading from base record for now
                page_range_id, main_page_id, slot_no = self.table.page_directory(rid)
                #read the old values before query
                for physical_page_id in range(1, self.table.num_columns+3):
                    page_id = "b" + str(page_range_id) + "-" + str(main_page_id) + "-" + str(physical_page_id) + "-"
                    curr_page = self.table.bufferpool.access(page_id, None)
                    value = curr_page.read(slot_no)
                    old_values.append(value)
                    fin.write(bytearray((value).to_bytes(8, 'big', signed=True)))
            query_entry_for_stack.append(old_values)
            
            #6. Adding new values to the log
            new_values = []
            #For indirection
            new_values.append(-1)
            fin.write(bytearray((-1).to_bytes(8, 'big', signed=True)))
            #For schema encoding
            new_values.append(0)
            fin.write(bytearray((0).to_bytes(8, 'big', signed=True)))
            #We can use -1, 0 for all functions initially because:
            #   1. delete - we don't care about these values for delete here
            #   2. insert - these are the correct values for insert
            #   3. update - we can use these temporarily, and change them to true values after the update runs
             
            for col_no in range(0, len(args)):
                if("delete" in str(query)):
                    new_values.append(0) 
                    fin.write(bytearray((0).to_bytes(8, 'big', signed=True)))
                #For update and insert
                else:
                    new_values.append(args[0][col_no])
                    fin.write(bytearray((args[0][col_no]).to_bytes(8, 'big', signed=True)))
            query_entry_for_stack.append(new_values)

            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            # If query was successful, now change the update's new values for indirection and schema 
            # as mentioned above while inserting new values
            if("update" in str(query)):
                #First get the primary key
                primary_key = args[0][0]
                rid = self.table.key_dict(primary_key)
                page_range_id, main_page_id, slot_no = self.table.page_directory(rid)

                #For the indirection page
                indirection_page_id = "b" + str(page_range_id) + "-" + str(main_page_id) + "-" + str(1) + "-"
                indirection_page = self.table.bufferpool.access(indirection_page_id, None)
                indirection_page.read(slot_no)

                #TODO: Now use these values to update the most recently added item on the stack and log
        return self.commit()

    def abort(self):
        #We start from the top of the stack. i.e. most recently inserted values for now
        while len(stack)>0:
            each_query_entry = self.stack.pop()
            action = each_query_entry[1]

            #1. Undo an insert
                #According to the instructions, any inserted records here need not be removed from the database
                #Can just be marked as deleted
            if action == "insert":
                #First get the primary key value which will be the third value in the list of new values
                #after indirection and schema encoding
                primary_key = each_query_entry[4][2]
                self.query_obj.delete(primary_key)
                #TODO: Do we need to mark the page dirty?

            #2. Undo a delete
            elif action == "delete":
                #get all old values and create a new record with those values
                #NOTE: Make sure the delete function from milestone 2 also inserts 00 for any deleted primary keys 
                # because the same primary key cannot be inserted twice             
                #along with the indirection values
                old_values = each_query_entry[3] 
                self.query_obj.insert(old_values)
                #TODO: We cannot insert old indirection and schema encoding values by using this insert function
                #      as it only takes data values and uses -1 and 0 for indirection and schema
                #      So access the actual pages to change these values

                #First get the rid from the primary key
                rid = self.table.key_dict(old_values[2])
                page_range_id, main_page_id, slot_no = self.table.page_directory(rid)

                #TODO: Do we need pincount adjustment here?
                #For the indirection page
                indirection_page_id = "b" + str(page_range_id) + "-" + str(main_page_id) + "-" + str(1) + "-"
                indirection_page = self.table.bufferpool.access(indirection_page_id, None)
                indirection_page.overwrite(slot_no, old_values[0])

                #For the schema page
                schema_page_id = "b" + str(page_range_id) + "-" + str(main_page_id) + "-" + str(2) + "-"
                schema_page = self.table.bufferpool.access(schema_page_id, None)
                schema_page.overwrite(slot_no, old_values[1])

                #Mark these pages as dirty
                #TODO: Make sure this function does the job
                self.table.bufferpool.mark_page_dirty(indirection_page_id)
                self.table.bufferpool.mark_page_dirty(schema_page_id)

            #3. Undo an update
            elif action == "update":
                
                pass
        return False

    def commit(self):
        # TODO: commit to database
        return True