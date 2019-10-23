import hashlib
import logging
import ntpath
import os
import socket

from os.path import join
from socketserver import TCPServer, StreamRequestHandler, ThreadingTCPServer

import zmq

fmt = '%(asctime)s  %(levelname)-5s :: %(message)s'
logging.basicConfig(level=logging.INFO, format=fmt)


class ClientMessage:
    def __init__(self, bytes_):
        self.valid_commands = set([
            "NO-OP/INVALID",
            "ARCHIVE",
            "SENDFILE",
            "SENDCHUNK",
            "GETFILE",
            "SEARCH"
        ])
        self.tokens = bytes_.split(b"<::>")
        num_toks = len(self.tokens)
        self.headers = ""
        if num_toks < 2:
            self.set_invalid_cmd()
        else:
            self.cmd = self.tokens[0].decode("utf8").strip()
            if num_toks == 2:
                self.message = self.tokens[1]
            elif num_toks == 3:
                self.headers = self.tokens[1].decode("utf8").strip()
                self.message = self.tokens[2]
            else:
                self.set_invalid_cmd()
        self.parse_headers()
        if self.cmd not in self.valid_commands:
            self.set_invalid_cmd()

    def set_invalid_cmd(self):
        self.cmd = "NO-OP/INVALID"
        self.message = b""

    def parse_headers(self):
        header_toks = self.headers.split("\n")
        headers = dict()
        for header in header_toks:
            header_kv = header.split(":")
            if len(header_kv) < 2:
                continue
            headers[header_kv[0].strip()] = header_kv[1]
        self.headers = headers

    def __repr__(self):
        return f"<ClientMessage:{self.cmd} :: {self.headers} :: {self.message}>"



class HpmTCPHandler(StreamRequestHandler):

    timeout = 35


    def handle_invalid(self, status_code=0):
        print(f"Responding with status code {status_code}")
        if status_code:
            self.send_status_code(status_code)
        print("Closing connection")
        self.request.shutdown(socket.SHUT_RDWR)
        self.request.close()

    def send_status_code(self, status_code):
        message_bytes = bytearray((status_code).to_bytes(2, "big"))
        self.request.sendall(message_bytes)        

    def handle_upload(self):
        filename_length = int.from_bytes(self.request.recv(1), "big")
        file_length = int.from_bytes(self.request.recv(8), "big")
        print(file_length, filename_length)
        filename = self.request.recv(filename_length).decode('utf8')
        expected_hash = self.request.recv(32)
        actual_hash = hashlib.sha256()
        received = 0
        print(f"File size: {file_length}, Filename length: {filename_length}, Filename: {filename}")
        write_to = join("serve", filename)
        if os.path.isfile(write_to):
            os.remove(write_to)
        while received < file_length:
            data = self.request.recv(1024)
            received += len(data)
            with open(write_to, "ab") as f:
                f.write(data)
                actual_hash.update(data)
        hashes_match = actual_hash.digest() == expected_hash
        print(f"Checking if the hashes match... {hashes_match}")
        if not hashes_match:
            print("Removing possibly corrupt file")
            os.remove(write_to)
            self.send_status_code(500)
        else:
            self.send_status_code(200)
        self.request.shutdown(socket.SHUT_RDWR)
        self.request.close()

    def handle_list(self):
        served = [os.path.join(os.getcwd(), "serve", file_) for file_ in os.listdir("serve")]
        if not served:
            no_files = bytearray((0).to_bytes(10, "big"))
            self.request.sendall(no_files)
        for item in served:
            delim = bytearray(b"<::>")
            item_name = ntpath.basename(item).encode("utf8")
            stat = os.stat(item)
            uploaded_on = bytearray(int(stat.st_mtime).to_bytes(8, "big"))
            size = bytearray((stat.st_size).to_bytes(8, "big"))
            item_ = bytearray()
            item_.extend(item_name)
            item_.extend(delim)
            item_.extend(uploaded_on)
            item_.extend(delim)
            item_.extend(size)
            
            payload_size = bytearray((len(item_)).to_bytes(10, "big"))
            print(f"Sending payload size {len(item_)} to client")
            self.request.sendall(payload_size)
            self.request.sendall(item_)
        self.request.shutdown(socket.SHUT_RDWR)
        self.request.close()

    def handle_download(self):
        query_length = int.from_bytes(self.request.recv(2), "big")
        query = self.request.recv(query_length).decode("utf8")
        served = [os.path.join(os.getcwd(), "serve", file_) for file_ in os.listdir("serve")]
        if not served:
            status = bytearray((404).to_bytes(2, "big"))
        else:
            status = bytearray((200).to_bytes(2, "big"))
        self.request.sendall(status)
        for item in served:
            basename = ntpath.basename(item)
            if query in basename:
                metadata = bytearray()
                basename_bytes = basename.encode("utf8")
                basename_size = bytearray(len(basename_bytes).to_bytes(1, "big"))
                item_size = bytearray((os.stat(item).st_size).to_bytes(8, "big"))
                metadata.extend(basename_size)
                metadata.extend(basename_bytes)
                metadata.extend(item_size)
                self.request.sendall(metadata)
                with open(item, "rb") as f:
                    self.request.sendfile(f)
                break
        self.request.shutdown(socket.SHUT_RDWR)
        self.request.close()
                
            
        
            
            


    def handle(self):
        cmd = int.from_bytes(self.request.recv(2), "big")
        if cmd == 200:
            self.handle_upload()
        elif cmd == 205:
            self.handle_list()
        elif cmd == 210:
            self.handle_download()
        else:
            print(f"Invalid command, got {cmd}")
            self.handle_invalid(400)
            return
        
    def handle_errors(self, request, client_address):
        print(f"Got a bad request from {client_address}")
        

        
if __name__ == '__main__':
    HOST, PORT = '0.0.0.0', 2499
    with ThreadingTCPServer((HOST, PORT), HpmTCPHandler) as server:
        print(f"Now listening on port {PORT}")
        server.serve_forever()
