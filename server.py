import threading
import zmq
from mvcc_editor import MVCCEditor

'''
server handles communication between client and editor
inputs: MVCC_Editor, zmq context
'''

def server(editor, context):
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")

    while True:
        message = socket.recv_json()

        if message["operation"] == "read":
            key = message["key"]
            result = editor.get(key)
            socket.send_json(result)

        elif message["operation"] == "write":
            key = message["key"]
            value = message["value"]
            transaction_id = editor.start_transaction()

            try:
                editor.set(key, value, transaction_id)
                # editor.commit(transaction_id)
                socket.send_json({"success": True, "message": "State is updated", "transaction_id": transaction_id})

            except Exception as e:
                editor.rollback(transaction_id)
                socket.send_json({"success": False, "message": f"Error: {e}"})

        elif message["operation"] == "rollback":
            socket.send_json({"success": True, "message": "Rolled back"})
        
        elif message["operation"] == "snapshot":
            key = message["key"]
            result = editor.getAll()
            socket.send_json(result)
            
        elif message["operation"] == "commit":
            key = message["key"]
            transaction_id = message["transaction_id"]
            
            try:
                transaction_id = editor.commit(transaction_id)
                socket.send_json({"success": True, "message": "Committed", "transaction_id": transaction_id})
            except Exception as e:
                editor.rollback(transaction_id)
                socket.send_json({"success": False, "message": f"Error: {e}"})
                
if __name__ == "__main__":
    filename = "mvcc.pkl"
    context = zmq.Context()
    editor = MVCCEditor(filename, context)

    editor.read()

    server_thread = threading.Thread(target=server, args=(editor, context))
    print("Starting server...")
    server_thread.start()

    try:
        server_thread.join()
    except KeyboardInterrupt:
        print("Terminating server...")

    context.term()
