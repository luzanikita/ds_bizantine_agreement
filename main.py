from pickle import TRUE
from client import Client
from threading import Thread
from twisted.internet import reactor
import socket
import copy
import argparse
import sys
import os
from config import DEBUG


host = "127.0.0.1"


def send(destination_id, string):
    if DEBUG:
        print("0", "->", destination_id, ":", string)

    port = 10000+destination_id
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)  
    s.sendto(string.encode('utf-8'), (host, port))


def is_int(value):
    try:
        int(value)
        return True
    except:
        return False


def command_input(command, clients, generals):
    command = command.split(" ")
    
    try:
        last_id = generals[-1]
        coordinator = clients[last_id].coordinator

        if command[0] == "g-state" and len(command) == 1:
            send(coordinator, "LIST-ALL")

        elif command[0] == "g-state" and len(command) > 2:
            assert is_int(command[1])
            assert command[2] in ["faulty", "non-faulty"]
            send(coordinator, ";".join(command))

        elif command[0] == "g-add" and len(command) > 1:
            assert is_int(command[1])
            for _ in range(int(command[1])):
                new_id = int(len(clients) + 1)
                generals.append(new_id)
                client = Client(
                    new_id, 
                    "localhost", 
                    "G"+str(new_id), 
                    copy.deepcopy(generals),
                    coordinator
                )
                clients[client.id] = client
                reactor.listenUDP(
                    10000+client.id, 
                    client
                )
                send(coordinator, f"ADD-ALL;{new_id}")

            send(coordinator, f"LIST-ALL")

        elif command[0] == "g-kill" and len(command) > 1:
            assert is_int(command[1])
            id_ = int(command[1])
            assert id_ in generals or id_ == coordinator
            if id_ in generals:
                generals.remove(id_)
            send(coordinator, ";".join(command))
        
        elif command[0] == "actual-order" and len(command) > 1:
            assert command[1] in ["attack", "retreat"]
            send(coordinator, ";".join(command))
        else:
            print("Wrong command!")

    except AssertionError:
        print("Wrong params!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', '-n', help='Number of processes', type=int, required=True)
    args = parser.parse_args()

    if args.processes < 1:
        print("Number of processes must be more than ZERO!")
        sys.exit()

    coordinator = 1
    generals = list(range(2, args.processes+1))

    clients = {
        id_: Client(
            id_, 
            "localhost", 
            "G"+str(id_), 
            copy.deepcopy(generals),
            coordinator
        ) for id_ in [coordinator] + generals
    }

    for id_, client in clients.items():
        reactor.listenUDP(
            10000+id_, 
            client
        )

    Thread(target=reactor.run, args=(False,), daemon=True).start()

    command = input()
    while command != "exit":
        command_input(command, clients, generals)
        command = input()
    
    os._exit(0)
