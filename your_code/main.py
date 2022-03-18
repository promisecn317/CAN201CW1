import struct
import threading
import time
import math
import os
from socket import *
import argparse
import json

encryption_on = "no"
socket_for_peer = {}  # Record the socket connection information of your peer
peer_status = {}  # Record peer status
total_file = {}  # All files in the current directory
new_add_file = {}  # Store newly added files
new_update_file = []  # Store updated files
new_file_from_peer = []  # Store files received from peer
new_update_from_peer = []  # Store files updated from peer
the_size_of_block = 1024 * 1024 * 2
share_file_directory = "share"
available_port_list = [23001, 23002, 23003, 23004, 23005, 23006, 23007, 23008, 23009]  # available port number
ip_list = []

# discover socket, which is used to control information with peers
port1 = 24001
socket1 = socket(AF_INET, SOCK_STREAM)
socket1.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
socket1.bind(('', port1))
socket1.listen(20)
# a connection used to transfer data
port2 = 24002
socket2 = socket(AF_INET, SOCK_STREAM)
socket2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
socket2.bind(('', port2))
socket2.listen(20)


# use to analyze the format of user commands
def _argparse():
    parser = argparse.ArgumentParser(description="This is description!")
    parser.add_argument('--ip', action='store', required=True,
                        dest='ip', help='ip addresses of peers')
    parser.add_argument('--encryption', action='store', required=False,
                        dest='encryption', help='use encryption transmission')
    return parser.parse_args()


# What is entered by the user
def inputValues():
    global ip_list, encryption_on
    parser = _argparse()
    ip_list = parser.ip.split(",")

    for ip in ip_list:
        peer_status[ip] = 0
    if parser.encryption is not None:
        encryption_on = parser.encryption

# get the block of file
def getFileBlock(fileName, block_index):
    global the_size_of_block
    f = open(fileName, 'rb')
    f.seek(block_index * the_size_of_block)
    file_block = f.read(the_size_of_block)
    f.close()
    return file_block


# get size of file
def getFileSize(fileName):
    return os.path.getsize(fileName)



def detectChange(ip_list):
    have_new_file = {}  # Inform peer that new documents have been added
    have_update_file = {}  # Notify peer of updated files
    for ip in ip_list:  # Traverse the ip list
        have_new_file[ip] = 0
        have_update_file[ip] = 0
    global total_file, new_add_file, new_update_file
    while True:
        total_file_after_detect = {}
        for root, dirs, files in os.walk(share_file_directory, followlinks=True):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    total_file_after_detect[file_path] = {"last_update_time": os.path.getmtime(file_path),
                                                          "file_size": getFileSize(file_path)}
                except:
                    total_file_after_detect[file_path] = {"last_update_time": total_file[file_path]["last_update_time"],
                                                          "file_size": total_file[file_path]["file_size"]}
        for file in total_file_after_detect:  # Traverse the current file information
            # total_file[file] == 1 means the file is being modified, the information about it makes no sense
            if file in total_file and total_file[file] == 1:
                total_file_after_detect[file] = 1
            # file is in total_file_after_update but not in total_file, it is a new added file
            if file not in total_file:
                new_add_file[file] = total_file_after_detect[file]
                total_file[file] = total_file_after_detect[file]
                for ip in ip_list:
                    have_new_file[ip] = 1
            if file in total_file and total_file[file] != 1:
                if total_file_after_detect[file]["last_update_time"] > total_file[file]["last_update_time"]:
                    total_file[file] = total_file_after_detect[file]
                    if file not in new_update_file:
                        new_update_file.append(file)
                        for ip in ip_list:
                            have_update_file[ip] = 1
        for ip in ip_list:
            if have_new_file[ip] == 1 and peer_status[ip] == 1:
                informNewFile(socket_for_peer[ip][0])  # Notify the user of updated files
                have_new_file[ip] = 0
            if have_update_file[ip] == 1 and peer_status[ip] == 1:
                informUpdateFile(socket_for_peer[ip][0])
                have_update_file[ip] = 0
        new_update_file = []



# Determine whether there are any new files, if not, need to update
def detectPeerNewFile(ip_address):
    global new_file_from_peer, new_update_from_peer
    while True:
        new_file = {}
        if new_file_from_peer:
            for index_i, value in enumerate(new_file_from_peer):
                if value["ip_address"] == ip_address:
                    new_file = new_file_from_peer.pop(index_i)
                    break
            if new_file:  # If there is a new file
                for index_i, value in enumerate(new_file_from_peer):
                    if value["file_name"] == new_file["file_name"]:
                        new_file_from_peer.pop(index_i)
                        break
                requestPeerNewFile(new_file, socket_for_peer[ip_address][1])
        if new_update_from_peer:
            new_file = {}
            for index_i, value in enumerate(new_update_from_peer):
                if value["ip_address"] == ip_address:
                    new_file = new_update_from_peer.pop(index_i)
            if new_file:
                requestUpdateFromPeer(new_file["file_name"], socket_for_peer[ip_address][1])



# Update some variable information when receiving the message.
# If the file is not local, it will be regarded as new.
# If the file size is different from the local file, you need to transfer it further.
def updatePeerNewFile(data, address):
    for file_name in data['new_add_file']:
        if file_name not in total_file:
            file = {'file_name': file_name, 'file_info': data["new_add_file"][file_name], "ip_address": address}
            if file not in new_file_from_peer:
                new_file_from_peer.append(file)
        if file_name in total_file and total_file[file_name] != 1 and getFileSize(file_name) < \
                data["new_add_file"][file_name]["file_size"]:
            furtherTransfer(socket_for_peer[address][1], file_name, data["new_add_file"][file_name]["file_size"])

# Client method
# Communicate with nearby equipment

def detectPeer(ip_address):
    try:
        socket_for_peer[ip_address][0].connect((ip_address, port1))
        socket_for_peer[ip_address][1].connect((ip_address, port2))
    except:
        peer_status[ip_address] = 0
        print(ip_address, "offline")
    else:
        peer_status[ip_address] = 1
        print(ip_address, "online")
        if new_add_file:  # If there is a new added file
            data = {"operation_code": 0, "server_operation_code": 1, "new_add_file": new_add_file}
            format_data = json.dumps(data).encode()
            encode_data = struct.pack('!I', len(format_data)) + format_data
            socket_for_peer[ip_address][0].send(encode_data)
        else:
            data = {"operation_code": 0, "server_operation_code": 0}
            format_data = json.dumps(data).encode()
            encode_data = struct.pack('!I', len(format_data)) + format_data
            socket_for_peer[ip_address][0].send(encode_data)
        msg = socket_for_peer[ip_address][0].recv(4)
        length = struct.unpack('!I', msg)[0]
        msg = socket_for_peer[ip_address][0].recv(length)
        unformatted_data = json.loads(msg.decode())
        if unformatted_data["server_operation_code"] == 1:
            updatePeerNewFile(unformatted_data, ip_address)


# Inform other users that local files have been added
def informNewFile(socket):
    data = {"operation_code": 1, "new_add_file": new_add_file}
    format_data = json.dumps(data).encode()
    encode_data = struct.pack('!I', len(format_data)) + format_data
    socket.send(encode_data)


# Inform other users that local files are updated
def informUpdateFile(socket):
    data = {"operation_code": 2, "new_update_file": new_update_file}
    format_data = json.dumps(data).encode()
    encode_data = struct.pack('!I', len(format_data)) + format_data
    socket.send(encode_data)


# Start getting new files from other users
def requestPeerNewFile(file, socket):
    time_start = time.time()
    print("start request")
    operation_code = 3
    file_name = file["file_name"]
    total_file[file_name] = 1
    total_file_size = file["file_info"]["file_size"]
    file_path = os.path.split(file_name)
    if not os.path.exists(file_path[0]):
        os.mkdir(file_path[0])
    rest_file_size = total_file_size
    total_block_number = math.ceil(total_file_size / the_size_of_block)
    print(total_block_number)
    f = open(file_name, 'wb')
    block_index = 0
    while rest_file_size > 0:  # The remaining files are not empty
        if block_index <= total_block_number:
            header = struct.pack('!II', operation_code, block_index)
            format_data = header + file_name.encode()
            header_length = len(format_data)
            binary_data = struct.pack('!I', header_length) + format_data
            socket.send(binary_data)
        if encryption_on == "no":
            msg = socket.recv(the_size_of_block * 3)
            f.write(msg)
            receive_data_size = len(msg)
            rest_file_size = rest_file_size - receive_data_size
            block_index += 1
        else:
            if rest_file_size >= the_size_of_block:
                rest_for_one_time = the_size_of_block
            else:
                rest_for_one_time = math.ceil(rest_file_size / 16) * 16
            text = b''
            while rest_for_one_time > 0:
                msg = socket.recv(rest_for_one_time)
                text += msg
                receive_data_size = len(msg)
                rest_for_one_time = rest_for_one_time - receive_data_size
            f.write(text)
            block_index += 1
            if rest_file_size >= the_size_of_block:
                rest_file_size = rest_file_size - the_size_of_block
            else:
                rest_file_size = 0
    f.close()
    time_end = time.time()
    total_file[file_name] = {"last_update_time": os.path.getmtime(file_name),
                             "file_size": getFileSize(file_name)}
    print(file_name, "finish in", time_end - time_start)


# Get the update file from peer
def requestUpdateFromPeer(file, socket):
    operation_code = 3
    file_name = file
    total_file[file_name] = 1
    f = open(file_name, 'rb+')
    rest_file_size = the_size_of_block
    header = struct.pack('!II', operation_code, 0)
    format_data = header + file_name.encode()
    header_length = len(format_data)
    binary_data = struct.pack('!I', header_length) + format_data
    socket.send(binary_data)
    text = b''
    while rest_file_size > 0:
        msg = socket.recv(the_size_of_block)
        text += msg
        receive_data_size = len(msg)
        rest_file_size = rest_file_size - receive_data_size
    f.write(text)
    f.close()
    total_file[file_name] = {"last_update_time": os.path.getmtime(file_name),
                             "file_size": getFileSize(file_name)}
    print(file_name, "update finish")


# Situations where further transfer of files is required
def furtherTransfer(socket, file_name, total_file_size):
    operation_code = 3
    total_file[file_name] = 1
    current_file_size = getFileSize(file_name)
    current_block_index = math.floor(current_file_size / the_size_of_block)
    rest_file_size = total_file_size - current_block_index * the_size_of_block
    total_block_number = math.ceil(total_file_size / the_size_of_block)
    request_block_index = current_block_index
    f = open(file_name, 'rb+')
    f.seek(current_block_index * the_size_of_block, 0)
    while rest_file_size > 0:
        if request_block_index <= total_block_number:
            header = struct.pack('!II', operation_code, request_block_index)
            format_data = header + file_name.encode()
            header_length = len(format_data)
            binary_data = struct.pack('!I', header_length) + format_data
            socket.send(binary_data)
        if encryption_on == "no":
            msg = socket.recv(the_size_of_block * 3)
            f.write(msg)
            receive_data_size = len(msg)
            rest_file_size = rest_file_size - receive_data_size
            request_block_index += 1
        else:
            if rest_file_size >= the_size_of_block:
                rest_for_one_time = the_size_of_block
            else:
                rest_for_one_time = math.ceil(rest_file_size / 16) * 16
            text = b''
            while rest_for_one_time > 0:
                msg = socket.recv(rest_for_one_time)
                text += msg
                receive_data_size = len(msg)
                rest_for_one_time = rest_for_one_time - receive_data_size
            f.write(text)
            request_block_index += 1
            if rest_file_size >= the_size_of_block:
                rest_file_size = rest_file_size - the_size_of_block
            else:
                rest_file_size = 0
    f.close()
    print("breakpoint resume finish")
    total_file[file_name] = {"last_update_time": os.path.getmtime(file_name),
                             "file_size": getFileSize(file_name)}


# Open a server connection
def startServerSocket1():
    while True:
        connection_socket1, address1 = socket1.accept()  # receive a new connection
        th = threading.Thread(target=subConnectionForInform, args=(connection_socket1, address1,))
        th.start()


# Innovative connection for data transmission
def startServerSocket2():
    while True:
        connection_socket2, address2 = socket2.accept()  # receive a new connection
        th = threading.Thread(target=subConnectionForTransfer, args=(connection_socket2, address2,))
        th.start()


# Loop, a thread that notifies other companions
def subConnectionForInform(connection_socket, address):
    while True:
        try:
            msg1 = connection_socket.recv(4)
        except:
            break
        else:
            if not msg1:
                break
            length = struct.unpack('!I', msg1)[0]
            msg1 = connection_socket.recv(length)
            processMessageForInform(msg1, connection_socket, address)


def subConnectionForTransfer(connection_socket, address):
    while True:
        try:
            message2 = connection_socket.recv(4)
        except:
            break
        else:
            if not message2:
                break
            binary_header_length = message2[:4]
            header_length = struct.unpack('!I', binary_header_length)[0]
            message2 = connection_socket.recv(header_length)
            try:
                processMessageForTransfer(message2, connection_socket)
            except:
                print(address[0], "offline")
                break


# Display the status of others, as well as the file information received
def processMessageForInform(message, connection_socket, address):
    decode_data = json.loads(message.decode())
    if decode_data["operation_code"] == 0:
        if peer_status[address[0]] == 1:  # 1 means the peer is online, but say hello again, so the peer was killed
            # need to reset the client socket and bind again
            peer_status[address[0]] = 0
            available_port_list.append(socket_for_peer[address[0]][0].getsockname()[1])
            socket_for_peer[address[0]][0].close()
            available_port_list.append(socket_for_peer[address[0]][1].getsockname()[1])
            socket_for_peer[address[0]][1].close()
            socket_for_peer[address[0]][0] = socket(AF_INET, SOCK_STREAM)
            socket_for_peer[address[0]][0].setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            socket_for_peer[address[0]][0].bind(("", available_port_list.pop()))
            socket_for_peer[address[0]][1] = socket(AF_INET, SOCK_STREAM)
            socket_for_peer[address[0]][1].setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            socket_for_peer[address[0]][1].bind(("", available_port_list.pop()))
        socket_for_peer[address[0]][0].connect((address[0], port1))
        socket_for_peer[address[0]][1].connect((address[0], port2))
        print(address[0], "online")
        peer_status[address[0]] = 1
        if decode_data["server_operation_code"] == 1:
            updatePeerNewFile(decode_data, address[0])
        if new_add_file:
            data = {"operation_code": 0, "server_operation_code": 1, "new_add_file": new_add_file}
            format_data = json.dumps(data).encode()
            encode_data = struct.pack('!I', len(format_data)) + format_data
            connection_socket.send(encode_data)
        else:
            data = {"operation_code": 0, "server_operation_code": 0}
            format_data = json.dumps(data).encode()
            encode_data = struct.pack('!I', len(format_data)) + format_data
            connection_socket.send(encode_data)
    elif decode_data["operation_code"] == 1:
        updatePeerNewFile(decode_data, address[0])
    elif decode_data["operation_code"] == 2:
        for file in decode_data["new_update_file"]:
            if file not in new_update_from_peer:
                new_update_from_peer.append({"file_name": file, "ip_address": address[0]})


def processMessageForTransfer(message, connection_socket):
    block_index = struct.unpack('!I', message[4:8])[0]
    file_name = message[8:].decode()
    file_block = getFileBlock(file_name, block_index)
    connection_socket.send(file_block)


if __name__ == '__main__':
    inputValues()
    t1 = threading.Thread(target=startServerSocket1)  # Create a new server thread
    t1.start()  # start thread
    t2 = threading.Thread(target=startServerSocket2)
    t2.start()
    t3 = threading.Thread(target=detectChange, args=(ip_list,))
    t3.start()
    for i in range(len(ip_list)):
        client_socket1 = socket(AF_INET, SOCK_STREAM)  # Initialize a new tcp connection
        client_socket1.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        bind_port1 = available_port_list.pop()
        client_socket1.bind(("", bind_port1))
        client_socket2 = socket(AF_INET, SOCK_STREAM)  # Initialize a new tcp connection
        client_socket2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        bind_port2 = available_port_list.pop()
        client_socket2.bind(("", bind_port2))
        socket_for_peer[ip_list[i]] = [client_socket1, client_socket2]
        detectPeer(ip_list[i])  # Check peer
        t4 = threading.Thread(target=detectPeerNewFile, args=(ip_list[i],))
        t4.start()
