import zmq
import threading
import uuid
import time


'''
handles communication between client and editor
generates unique client key commits to our k-v store
inputs: MVCC_Editor, zmq context
'''

def client(client_id):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")

    client_key = str(uuid.uuid4())
    print(f"Client {client_id} connected")

    try:
        transaction_id = None

        while True:
            action = input(f"Client {client_id}: Enter action (read, write, rollback, exit): ")

            if action == "exit":
                break

            if action == "read":
                key = client_key
                if transaction_id:
                    key_result = input(f"Client {client_id}: Enter key to read (leave blank for previous key): ")
                    key = key_result if key_result else key

                socket.send_json({"operation": action, "key": key, "transaction_id": transaction_id})
                result = socket.recv_json()
                print(f"Client {client_id}: Read - Result: {result}")

            elif action == "write":
                key = client_key
                value = input(f"Client {client_id}: Enter value for write action: ")

                socket.send_json({"operation": action, "key": key, "value": value})
                result = socket.recv_json()

                if result["success"]:
                    transaction_id = None  # Reset transaction after successful write

                print(f"Client {client_id}: Write - Value: {value} - Result: {result}")

            elif action == "rollback":
                socket.send_json({"operation": "rollback"})
                result = socket.recv_json()
                transaction_id = None  # Reset transaction after rollback
                print(f"Client {client_id}: Rollback - Result: {result}")

    except KeyboardInterrupt:
        print(f"Client {client_id}: Terminating...")

    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    num_clients = int(input("Enter the number of clients: "))

    threads = []
    for i in range(num_clients):
        client_thread = threading.Thread(target=client, args=(i + 1,))
        threads.append(client_thread)
        client_thread.start()

    for thread in threads:
        thread.join()

    print("All clients terminated.")