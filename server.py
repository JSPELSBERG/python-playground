import fib
import socket

def fib_server(address):
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        sock.bind(address)
        sock.listen(5)
        while True:
            client, addr = sock.accept()
            print("Connection ",addr)
            fib_handler(client)
        
def fib_handler(client):
    while True:
        req = client.recv(100)
        if not req:
            break
        n = int(req)
        result = fib.fib(n)
        resp = str(result).encode("ascii") + b"\n"
        client.send(resp)
    print("Closed")

fib_server(("",25000))
