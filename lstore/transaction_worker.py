from lstore.table import Table, Record
from lstore.index import Index
import threading
from time import *


class TransactionWorker:

    """ 
    # Creates a transaction worker object.
    """
    def __init__(self, id, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.t = None
        self.worker_id = id
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """
    def run(self):
        # here you need to create a thread and call __run
        self.t = threading.Thread(target=self.__run, daemon=False)
        print("Starting thread: " + str(self.worker_id))
        self.t.start()
    
    """
    Waits for the worker to finish
    """
    def join(self):
        print("In join of: " + str(self.worker_id) )
        self.t.join()
        print("Join done for: " + str(self.worker_id))

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run(self.worker_id))
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
