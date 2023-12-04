import pickle

from collections import defaultdict
import uuid

'''
MVCCEditor implements a simple MVCC editor, 
by internally maintaining a key-value store that supports transactions.
It is used by our server to store the state of the file.

We have decided to use ZMQ (REQ-REP) for our communication protocol.

Data structure:
    * data: dictionary that stores the k-v pairs. if key is not present, value is assigned as None.
    * transactions: dictionary that stores the transaction id as key and the changes as value, only for
        active transactions (i.e. those that have not been committed).
        After a transaction is committed, the changes are stored in the data dictionary, and the 
        transaction is removed from the transactions dictionary to free up memory.
    * transaction_counter: a counter that keeps track of the number of transactions.
    * context: the zmq context.
    * filename: the name of the file that stores the data.
'''

class MVCCEditor:
    def __init__(self, filename, context):
        self.filename = filename
        self.transaction_counter = 0
        self.data = defaultdict(lambda: None)
        self.transactions = {}
        self.context = context

    '''
    reads data from file using pickle
    if file empty or doesn't exist, intialize empty dict
    '''
    def read(self):
        try:
            with open(self.filename, 'rb') as file:
                self.data = pickle.load(file)
        except (EOFError, FileNotFoundError):
            self.data = {}

    '''
    writes data to file using pickle
    '''
    def write(self):
        with open(self.filename, 'wb') as file:
            pickle.dump(self.data, file)

    '''
    initiates a new transaction
    adds transaction with transaction_id as key and initializes value as empty dictionary
    returns the generated transaction id
    '''
    def start_transaction(self):
        transaction_id = str(uuid.uuid4())
        self.transactions[transaction_id] = {'snapshot': self.data.copy(), 'changes': defaultdict(lambda: None)}
        return transaction_id

    '''
    commits the changes of the transaction to data
    removes the transaction from the transactions dictionary to free up memory
    '''
    def commit(self, transaction_id):
        if transaction_id not in self.transactions:
            raise KeyError(f"Transaction ID {transaction_id} not found")

        changes = self.transactions[transaction_id]['changes']

        for key, value in changes.items():
            self.data[key] = value

        self.write()
        del self.transactions[transaction_id]


    '''
    rolls back the changes of the transaction
    removes the transaction from the transactions dictionary to free up memory
    '''
    def rollback(self, transaction_id):
        if transaction_id not in self.transactions:
            raise KeyError(f"Transaction ID {transaction_id} not found")

        self.data = self.transactions[transaction_id]['snapshot']
        del self.transactions[transaction_id]
    '''
    helper method to set the value of a key in the transaction
    '''
    def set(self, key, value, transaction_id):
        if key is None:
            raise KeyError("Invalid key: None")
        self.transactions[transaction_id]['changes'][key] = value

    '''
    helper method to get the value of a key
    '''
    def get(self, key, transaction_id=None):
        if transaction_id and key in self.transactions[transaction_id]['changes']:
            return self.transactions[transaction_id]['changes'][key]
        else:
            return self.data.get(key, None)
