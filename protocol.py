import socket
import threading
import collections


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(
            family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def close(self):
        self.udp_socket.close()


class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.udp_socket.settimeout(0.0001)
        self.seq = 0
        self.ack = 0
        self.saved_ack_data = b''
        self.saved_receive_data = b''
        self.saved_data_lock = threading.Lock()
        self.buffer_size = 10000
        self.active = True
        self.send_lock = threading.Condition()
        self.recv_lock = threading.Condition()
        self.recv_result_lock = threading.Condition()
        self.ack_check_lock = threading.Lock()
        self.data_to_send = collections.deque()
        self.num_to_receive = 0
        self.received_data = b''

        self.send_thread = threading.Thread(target=self.send_call)
        self.recv_thread = threading.Thread(target=self.recv_call)
        self.send_thread.daemon = True
        self.recv_thread.daemon = True
        self.send_thread.start()
        self.recv_thread.start()

    @staticmethod
    def create_packet(num_of_bytes, seq, data):
        return int.to_bytes(1, 1, 'big') + int.to_bytes(num_of_bytes, 4, 'big') + int.to_bytes(seq, 4, 'big') + data

    @staticmethod
    def create_message_with_ack(ack):
        return int.to_bytes(0, 1, 'big') + int.to_bytes(ack, 4, 'big')

    @staticmethod
    def parse_message_with_ack(data):
        return int.from_bytes(data[0: 1], 'big'), int.from_bytes(data[1: 5], 'big')

    @staticmethod
    def parse_message_with_data(data):
        return int.from_bytes(data[0: 1], 'big'), int.from_bytes(data[1: 5], 'big'), int.from_bytes(data[5: 9], 'big'), data[9: len(data)]

    def send_call(self):
        current_length_of_data = -1
        while self.active:

            data = b''
            with self.send_lock:
                while len(self.data_to_send) == 0 and self.active:
                    self.send_lock.wait()

                if not self.active:
                    break

                if current_length_of_data == -1:
                    current_length_of_data = len(self.data_to_send[0])

                length = min(
                    len(self.data_to_send[0]), self.buffer_size)
                data += self.data_to_send[0][0:length]
                self.data_to_send[0] = self.data_to_send[0][length:]

                packet = MyTCPProtocol.create_packet(
                    current_length_of_data, self.seq, data)

                if len(self.data_to_send[0]) == 0:
                    self.data_to_send.popleft()
                    current_length_of_data = -1

            while True:
                final_flag = False
                interim = 0
                while True:
                    if not self.active:
                        return
                    interim += 1
                    interim = min(interim, 1)
                    for _ in range(interim):
                        self.sendto(packet)
                    while True:
                        received = b''
                        try:
                            received = self.recvfrom(self.buffer_size + 10)
                        except Exception as e:
                            with self.ack_check_lock:
                                if len(self.saved_ack_data) != 0:
                                    received = self.saved_ack_data
                                    self.saved_ack_data = b''
                                else:
                                    break
                        flag, ack = MyTCPProtocol.parse_message_with_ack(
                            received)
                        if flag != 0:
                            with self.saved_data_lock:
                                self.saved_receive_data = received
                            continue

                        if ack > self.seq:
                            self.seq = ack
                            final_flag = True
                            break

                        break
                    if final_flag:
                        break
                if final_flag:
                    break

    def send(self, data: bytes):
        with self.send_lock:
            self.data_to_send.append(data)
            self.send_lock.notify_all()
        return len(data)

    def recv_call(self):

        while self.active:
            result = b''
            with self.recv_lock:
                while self.active and self.num_to_receive == 0:
                    self.recv_lock.wait()
                if not self.active:
                    break
            prev_ack = self.ack
            while True:
                try:
                    received = self.recvfrom(self.buffer_size + 10)
                except Exception as e:
                    with self.saved_data_lock:
                        if len(self.saved_receive_data) != 0:
                            received = self.saved_receive_data
                            self.saved_receive_data = b''
                        else:
                            continue
                flag, num_of_bytes, seq, data = MyTCPProtocol.parse_message_with_data(
                    received)
                if flag != 1:
                    with self.ack_check_lock:
                        self.saved_ack_data = received
                    continue

                if seq != self.ack or (len(data) != self.buffer_size and len(data) != num_of_bytes - (self.ack - prev_ack)):

                    message = MyTCPProtocol.create_message_with_ack(
                        self.ack)
                    self.sendto(message)
                else:
                    self.ack += len(data)
                    result += data
                    message = MyTCPProtocol.create_message_with_ack(
                        self.ack)
                    self.sendto(message)
                if self.ack - prev_ack == num_of_bytes:
                    for _ in range(5):
                        self.sendto(message)
                    break

            self.received_data = result
            self.num_to_receive = 0
            with self.recv_result_lock:
                self.recv_result_lock.notify_all()
        return result

    def recv(self, n: int):
        result = bytes()
        with self.recv_lock:
            self.num_to_receive = n
            self.recv_lock.notify_all()
        with self.recv_result_lock:
            while len(self.received_data) == 0:
                self.recv_result_lock.wait()

                result += self.received_data
                self.received_data = b''
                return result

    def close(self):
        self.active = False
        with self.send_lock:
            self.send_lock.notify_all()
        with self.recv_lock:
            self.recv_lock.notify_all()
        self.send_thread.join()
        self.recv_thread.join()
        super().close()
