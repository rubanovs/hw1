import socket
import time
from queue import PriorityQueue

HEADER_SIZE = 16
TIMEOUT = 0.01

class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg
        
    def close(self):
        self.udp_socket.close()



class Segment:

    def __init__(self, seq: int, ack: int, data: bytes):
        self.data = data
        self.seq = seq
        self.ack = ack
        self.send_time = time.time()
        self.isConnected = False

    def __len__(self):
        return len(self.data)

    def __eq__(self, other):
        return self.seq == other.seq
    
    def __lt__(self, other):
        return self.seq < other.seq


    def encode(self) -> bytes:
        return self.seq.to_bytes(8, "big", signed=False) + self.ack.to_bytes(8, "big", signed=False) + self.data
    
    @staticmethod
    def decode(data: bytes) -> 'Segment':
        return Segment(int.from_bytes(data[:8], "big", signed=False), int.from_bytes(data[8:16], "big", signed=False), data[HEADER_SIZE:])

    @property
    def timeodOut(self):
        if not self.isConnected and (time.time() - self.send_time > TIMEOUT):
            return True
        return False



class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, client='Client', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buff_size = 1024
        self.window_size = 5000
        self.client = client
        self.sent_byte = 0
        self.recv_byte = 0
        self.ack_bytes = 0
        self.buf = bytes()
        self.send_window = PriorityQueue()
        self.recv_window = PriorityQueue()


    def SendSegment(self, segment: Segment) -> int:
        self.udp_socket.settimeout(None)
        bytes_sent = self.sendto(segment.encode()) - HEADER_SIZE

        if len(segment):
            segment.data = segment.data[: bytes_sent]
         
            segment.send_time = time.time()
            self.send_window.put((segment.seq, segment), block=False)


        if segment.seq == self.sent_byte:
            self.sent_byte += bytes_sent

        return bytes_sent

    def send(self, data: bytes) -> int:
        counter = 0
        ack_attempts = 0
        while (data or self.ack_bytes < self.sent_byte):
            if self.sent_byte - self.ack_bytes <= self.window_size and data:
                end = min(self.buff_size, len(data))
                data_size = self.SendSegment(Segment(self.sent_byte, self.recv_byte, data[: end]))
                data = data[data_size:]
                counter += data_size
                self.RecvSegment(0.)
            else:
                if self.RecvSegment(TIMEOUT):
                    ack_attempts = 0
                else:
                    ack_attempts += 1

            if not self.send_window.empty():
                _, first_segment = self.send_window.get(block=False)
                if first_segment.timeodOut:
                    self.SendSegment(first_segment)
                else:
                    self.send_window.put((first_segment.seq, first_segment), block=False)


        return counter



    def RecvSegment(self, timeout: float = None) -> bool:
        self.udp_socket.settimeout(timeout)
        try:
            segment = Segment.decode(self.recvfrom(self.buff_size + HEADER_SIZE))
        except socket.error:
            return False

        if len(segment):
            self.recv_window.put((segment.seq, segment), block=False)
            first_segment = None
            while not self.recv_window.empty():
                _, first_segment = self.recv_window.get(block=False)
                if first_segment.seq < self.recv_byte:
                    first_segment.isConnected = True
                elif first_segment.seq == self.recv_byte:
                    self.buf += first_segment.data
                    self.recv_byte += len(first_segment)
                    first_segment.isConnected = True
                else:
                    self.recv_window.put((first_segment.seq, first_segment), block=False)
                    break

            if first_segment is not None:
                self.SendSegment(Segment(self.sent_byte, self.recv_byte, bytes()))


        if segment.ack > self.ack_bytes:
            self.ack_bytes = segment.ack
            while not self.send_window.empty():
                _, first_segment = self.send_window.get(block=False)
                if first_segment.seq >= self.ack_bytes:
                    self.send_window.put((first_segment.seq, first_segment), block=False)
                    break

        return True
    

    def recv(self, n: int) -> bytes:
        end = min(n, len(self.buf))
        data = self.buf[:end]
        self.buf = self.buf[end:]
        while len(data) < n:
            self.RecvSegment()
            end = min(n, len(self.buf))
            data += self.buf[:end]
            self.buf = self.buf[end:]

        return data

    def close(self):
        super().close()
