from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from time import sleep
from config import DEBUG
from random import choice


class Client(DatagramProtocol):
    def __init__(self, id, host, name, participants, coordinator_id):
        if host == "localhost":
            self.host = "127.0.0.1"

        self.name = name
        self.id = id
        self.alive = True
        self.ready = []
        self.participants = participants
        self.waiting_state = False
        self.waiting_counter = 0
        self.respond_state = True
        self.listen_state = True
        self.coordinator = coordinator_id
        self.faulty = False
        self.heartbeat = 300
        self.heartbeat_counter = 0
        self.election_running = False
        self.faulty_nodes = []

    def get_state(self):
        return "F" if self.faulty else "NF"

    def get_rank(self):
        return "primary" if self.id == self.coordinator else "secondary"

    def datagramReceived(self, datagram: bytes, addr):
        datagram = datagram.decode('utf-8')
        # try:
        self.process_datagram(datagram)
        # except:
        #     print("Command canceled!")
    
    def startProtocol(self):
        reactor.callInThread(self.time_tick)

    def time_tick(self):
        while self.alive:
            sleep(0.01)

            self.heartbeat_counter += 1
            if self.heartbeat_counter > self.heartbeat and not self.election_running:
                self.check_heartbeat()
                self.heartbeat_counter = 0

            if self.waiting_state:
                self.waiting_counter += 1
                if self.waiting_counter > 100:
                    self.waiting_state = False
                    self.waiting_counter = 0
                    if self.election_running:
                        self.end_election()
                    elif self.respond_state:
                        self.start_election()

    def process_datagram(self, string):
        splits = string.split(";")
        function = splits[0]
        args = splits[1:]
        
        if not self.listen_state:
            print(f"{self.name} is not responding!")
            return 
        
        if function == "g-state" and not self.election_running:
            id_ = int(args[0])
            state = args[1]
            self.send(id_, f"SET;{state}")

        elif function == "SET":
            state = args[0]
            self.faulty = state == "faulty"
            self.send(self.coordinator, "LIST-ALL;")

        
        elif function == "LIST-ALL" and not self.election_running:
            print(f"{self.name}, {self.get_rank()}, state={self.get_state()}")
            self.broadcast("LIST;")

        elif function == "LIST":
            print(f"{self.name}, {self.get_rank()}, state={self.get_state()}")

    
        elif function == "ADD-ALL" and not self.election_running:
            id_ = int(args[0])
            if id_ not in self.participants:
                self.participants.append(id_)
            self.broadcast(f"ADD;{id_}")
        
        elif function == "ADD":
            id_ = int(args[0])
            if id_ not in self.participants:
                self.participants.append(id_)
            if self.coordinator in self.participants:
                self.participants.remove(self.coordinator)

        elif function == "g-kill" and not self.election_running:
            id_ = int(args[0])
            if id_ == self.coordinator:
                self.listen_state = False
                self.respond_state = False
                self.waiting_state = False
            else:
                self.broadcast(f"REMOVE;{id_}")
                self.participants.remove(id_)
                self.send(self.id, "LIST-ALL;")
        
        elif function == "REMOVE":
            id_ = int(args[0])
            if id_ == self.id:
                self.listen_state = False
                self.respond_state = False
                self.waiting_state = False
                self.waiting_counter = 0
            self.participants.remove(id_)
        
        elif function == "HEARTBEAT":
            self.send(int(args[0]), "HEARTBEAT-OK;")
        
        elif function == "HEARTBEAT-OK":
            self.waiting_state = False
            self.waiting_counter = 0
        
        elif function == "ELECTION-START":
            self.send(int(args[0]), f"ELECTION-OK;{self.id}")
        
        elif function == "ELECTION-OK":
            self.waiting_state = False
            self.waiting_counter = 0
        
        elif function == "ELECTION-END":
            self.election_running = False
            coordinator_id = int(args[0])
            self.participants.remove(coordinator_id)
            self.coordinator = coordinator_id

        elif function == "actual-order" and not self.election_running:
            self.order = args[0]
            print(f"{self.name}, {self.get_rank()}, majority={self.order}, state={self.get_state()}")

            self.ready = []
            self.faulty_nodes = [self.faulty]
            for id_ in self.participants:
                order = self.random_order() if self.faulty else self.order
                self.send(id_, f"CONSENSUS-START;{order};{self.get_state()}")
        
        elif function == "CONSENSUS-START":
            self.order = args[0]
            coordinator_state = args[1] == "F"
            self.ready = []
            self.faulty_nodes = [coordinator_state]
            self.broadcast(f"CONSENSUS-EXCHANGE;{self.order};{self.get_state()}")

        elif function == "CONSENSUS-EXCHANGE":
            order = args[0]
            state = args[1] == "F"
            self.ready.append(order)
            self.faulty_nodes.append(state)

            if len(self.ready) == len(self.participants):
                k = sum(self.faulty_nodes)
                if len(self.participants) < 3 * k:
                    self.order = "undefined"
                else:
                    self.order, _ = self.get_final_order(self.ready)
                self.end_consensus()
            
        elif function == "CONSENSUS-END":
            order = args[0]
            state = args[1] == "F"
            if state:
                order = "undefined"
            self.ready.append(order)
            self.faulty_nodes.append(state)
            if len(self.ready) == len(self.participants):
                order, count = self.get_final_order(self.ready)
                k = sum(self.faulty_nodes)
                n = len(self.participants) + 1
                if order == "undefined":
                    print(f"Execute order: cannot be determined – not enough generals in the system! {k} faulty node in the system - {count} out of {n} quorum not consistent")
                else:
                    print(f"Execute order: {order}! {k} faulty nodes in the system – {count} out of {n} quorum suggest {order}")

    def end_consensus(self):
        print(f"{self.name}, {self.get_rank()}, majority={self.order}, state={self.get_state()}")
        self.send(self.coordinator, f"CONSENSUS-END;{self.order};{self.get_state()}")
    
    def get_final_order(self, order_list):
        poll = {}
        for order in order_list:
            poll[order] = poll.get(order, 0) + 1
        
        final_order = None
        final_count = -1
        for order, count in poll.items():
            if count > final_count:
                final_order = order
                final_count = count
        
        return final_order, final_count

    def random_order(self):
        return choice(["attack", "retreat"])

    def check_heartbeat(self):
        if self.id != self.coordinator:
            self.waiting_state = True
            self.send(self.coordinator, f"HEARTBEAT;{self.id}")

    def start_election(self):
        print(f"{self.name} joined election")
        self.election_running = True
        self.waiting_counter = 0
        self.waiting_state = True
        for p_id in self.participants:
            if p_id > self.id:
                self.send(p_id, f"ELECTION-START;{self.id}")
    
    def end_election(self):
        self.election_running = False
        self.waiting_counter = 0
        self.waiting_state = False
        self.coordinator = self.id
        self.participants.remove(self.id)
        self.broadcast(f"ELECTION-END;{self.id}")
        print(f"{self.name} was elected")
        self.send(self.id, "LIST-ALL;")

    def send(self, destination_id, string):
        if not self.respond_state:
            return

        if DEBUG:
            print(self.id, "->", destination_id, ":", string)

        addr = self.host, 10000+destination_id
        self.transport.write(string.encode('utf-8'), addr)
    
    def broadcast(self, string):
        for id in self.participants:
            self.send(id, string)
