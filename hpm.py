import argparse
import datetime
import hashlib
import json
import logging
import ntpath
import os
import socket
import sys
import tarfile

fmt = '%(asctime)s  :: %(message)s'
logging.basicConfig(level=logging.INFO, format=fmt)

parser = argparse.ArgumentParser(description="A minimal package manager to set up a new machine on a local network.")
parser.add_argument("-s", "--server", help="the server URL", required=True)
parser.add_argument("-i", "--install", help="install a package")
parser.add_argument("-d", "--download", help="download a folder from the server")
parser.add_argument("-p", "--package", help="package a folder or file")
parser.add_argument("-u", "--publish", help="publish a folder or file to the remote server")
parser.add_argument("-l", "--list", action="store_true", help="publish a folder or file to the remote server")
parser.add_argument("-o", "--out", help="the download or install destination.")
args = parser.parse_args()

CHUNKSIZE = 1048

class HpmClient:
    def __init__(self, server):
        self.server = server

    def list_files(self, filter_term):
        pass

    def download_file(self, file, destination=None):
        if not destination:
            destination = os.getcwd()
        logging.info(f"Downloading {file} to {destination} from {self.server}")
        self.send_tcp(f"download::{file}")

    def package_file(self, file_path):
        basename = ntpath.basename(file_path)
        archive_name = f"{basename}.tar.gz"
        with tarfile.open(name=archive_name, mode="w:gz") as tf:
            tf.add(file_path, arcname=basename)
        logging.info(f"Packaged {basename} into {archive_name}")
        return archive_name

    def create_header(self, cmd, arg, message):
        header = bytearray(cmd.encode('utf8'))
        separator = bytearray(b"<::>")
        arg = bytearray(arg.encode('utf8'))
        msg = bytearray(message)
        header.extend(separator)
        header.extend(arg)
        header.extend(separator)
        header.extend(msg)
        return header

    def get_hash(self, file_path):
        hash_ = hashlib.sha256()
        with open(file_path, "rb") as f:
            data = f.read(CHUNKSIZE)
            while data:
                hash_.update(data)
                data = f.read(CHUNKSIZE)
        return hash_.digest()

    def list_server(self):
        cmd = bytearray((205).to_bytes(2, "big"))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.server, 2499))
            sock.send(cmd)
            items = []
            resp_size = int.from_bytes(sock.recv(10), "big")
            if not resp_size:
                print("No items available")
            while resp_size:
                current = sock.recv(resp_size)
                item = current.split(b"<::>")
                name = item[0].decode("utf8")
                uploaded_on = int.from_bytes(item[1], "big")
                size = int.from_bytes(item[2], "big")
                items.append({
                    "name": name,
                    "uploaded_on": datetime.datetime.fromtimestamp(uploaded_on).strftime("%B %d, %Y at %I:%M %p"),
                    "size": size
                })
                resp_size = int.from_bytes(sock.recv(10), "big")
            if items:
                print(f"{'Item':<25} {'Uploaded On':<25} {'Bytes':>12}")
                print("-------------------------------------------------------------------")
                for item in items:
                    print(f"{item['name']:<25} {item['uploaded_on']:>25} {item['size']:>12}")
            else:
                print("No items available")    

    def publish_file(self, file_path):
        logging.info("Packaging file")
        archive = self.package_file(file_path)
        basename = ntpath.basename(archive)
        basename_bytes = basename.encode("utf8")
        header = bytearray()
        cmd = bytearray((200).to_bytes(2, "big"))
        file_size_ = os.stat(archive).st_size
        file_size = bytearray((file_size_).to_bytes(8, "big"))
        filename_size_ = len(basename_bytes)
        filename_size = bytearray((filename_size_).to_bytes(1, "big"))
        print(f"File size: {file_size_}, Filename length: {filename_size_}, Filename: {basename}")
        header.extend(cmd)
        header.extend(filename_size)
        header.extend(file_size)
        header.extend(basename_bytes)
        logging.info("Getting SHA256 of archive")
        header.extend(self.get_hash(archive))
        print(f"Sending {basename}, ")
        with open(archive, "rb") as f:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.server, 2499))
                sock.send(header)
                sock.sendfile(f)
                response_code = int.from_bytes(sock.recv(2), "big")
                if response_code > 299:
                    print(f"Upload failed. The remote server responded with code {response_code}")
                elif response_code == 200:
                    print("Upload succeeded!")
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()

    def download(self, query):
        query = query.encode("utf8")
        cmd = query_length = bytearray((210).to_bytes(2, "big"))
        query_length = bytearray(len(query).to_bytes(2, "big"))
        header = bytearray()
        header.extend(cmd)
        header.extend(query_length)
        header.extend(query)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.server, 2499))
            sock.send(header)
            status = int.from_bytes(sock.recv(2), "big")
            if status > 399:
                print("No files available")
                return
            else:
                filename_length = int.from_bytes(sock.recv(1), "big")
                filename = sock.recv(filename_length).decode('utf8')
                file_length = int.from_bytes(sock.recv(8), "big")
                received = 0
                write_to = os.path.join(os.getcwd(), filename)
                print(f"Downloading {filename} {filename_length} to {write_to} {file_length}")
                if os.path.isfile(write_to):
                    os.remove(write_to)
                while received < file_length:
                    data = sock.recv(1024)
                    received += len(data)
                    with open(write_to, "ab") as f:
                        f.write(data)
                if filename_length and filename and file_length:
                    with tarfile.open(write_to) as tf:
                        def is_within_directory(directory, target):
                            
                            abs_directory = os.path.abspath(directory)
                            abs_target = os.path.abspath(target)
                        
                            prefix = os.path.commonprefix([abs_directory, abs_target])
                            
                            return prefix == abs_directory
                        
                        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                        
                            for member in tar.getmembers():
                                member_path = os.path.join(path, member.name)
                                if not is_within_directory(path, member_path):
                                    raise Exception("Attempted Path Traversal in Tar File")
                        
                            tar.extractall(path, members, numeric_owner=numeric_owner) 
                            
                        
                        safe_extract(tf)
                        tf.close()
                    os.remove(write_to)
                        

        
        
                    

if __name__ == '__main__':
    client = HpmClient(args.server)
    if args.download:
        client.download(args.download)
    if args.package:
        client.package_file(args.package)
    if args.publish:
        client.publish_file(args.publish)
    if args.list:
        client.list_server()
