from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from time import sleep
from config import DEBUG


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
        self.heartbeat = 500
        self.heartbeat_counter = 0
        self.election_running = False
    
    def get_state(self):
        return "F" if self.faulty else "NF"

    def get_rank(self):
        return "primary" if self.id == self.coordinator else "secondary"

    def datagramReceived(self, datagram: bytes, addr):
        datagram = datagram.decode('utf-8')
        self.process_datagram(datagram)
    
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
        
        if function == "g-state":
            self.send(int(args[0]), f"SET;{args[1]}")

        elif function == "SET":
            self.faulty = args[0] == "faulty"
        
        elif function == "LIST-ALL":
            print(f"{self.name}, {self.get_rank()}, state={self.get_state()}")
            self.broadcast("LIST;")

        elif function == "LIST":
            print(f"{self.name}, {self.get_rank()}, state={self.get_state()}")
    
        elif function == "ADD-ALL":
            self.participants.append(int(args[0]))
            self.broadcast(f"ADD;{args[0]}")
        
        elif function == "ADD":
            self.participants.append(int(args[0]))

        elif function == "g-kill":
            id_ = int(args[0])
            if id_ == self.coordinator:
                self.listen_state = False
                self.respond_state = False
                self.waiting_state = False
            else:
                self.broadcast(f"REMOVE;{id_}")
                self.participants.remove(id_)
        
        elif function == "REMOVE":
            id_ = int(args[0])
            if id_ == self.id:
                self.listen_state = False
                self.respond_state = False
                self.waiting_state = False
                self.waiting_counter = 0
            self.participants.remove(int(args[0]))
        
        elif function == "HEARTBEAT":
            self.send(int(args[0]), "HEARTBEAT-OK;")
        
        elif function == "HEARTBEAT-OK":
            self.waiting_state = False
            self.waiting_counter = 0
        
        elif function == "ELECTION-START":
            self.ready = []
            self.send(int(args[0]), f"ELECTION-OK;{self.id}")
        
        elif function == "ELECTION-OK":
            self.ready.append(int(args[0]))
            self.waiting_state = False
            self.waiting_counter = 0
        
        elif function == "ELECTION-END":
            self.election_running = False
            coordinator_id = int(args[0])
            self.participants.remove(coordinator_id)
            self.coordinator = coordinator_id

    def check_heartbeat(self):
        if self.id != self.coordinator:
            self.waiting_state = True
            self.send(self.coordinator, f"HEARTBEAT;{self.id}")

    def start_election(self):
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
