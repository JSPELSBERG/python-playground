import socket
import time

sock = socket.socket()
sock.connect(("localhost",25000))

while True:
    start = time.time()
    sock.send(b"30")
    resp = sock.recv(100)
    end = time.time()
    print(end-start)
