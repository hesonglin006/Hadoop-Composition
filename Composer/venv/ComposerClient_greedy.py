from __future__ import division
import socket, pickle
import threading
import SocketServer
import os
import shutil
import time
import sys
import struct
import math

from SocketCom import Socket

BUFSIZE = 4096
SCHEDULER_PORT = 27006
LISTEN_HOST = '0.0.0.0'
FILE_PORT = 27008
COMPOSER_PORT = 47006
SCHEDULING_CENTER = 'xx.xx.xx.xx'

dataBlockMin = 64
dataBlockMax = 128

class Cons():
    # composition mode: 1, n; 2, nlogn; 3, n^2.
    COMPOSITION_MODE = int(sys.argv[1])
    COMPOSITION_CONSTANT = 1 / 1000
    TOTAL_SIZE = 0
    COMPOSED_AVAILABLE_TIME = 0
    COMPOSITION_TIME = 2
    CURRENT_NODE = {}

class PayLoad():
    ID = ""
    MODE = ""
    SENDING_FLAG = ""
    SOURCE = None
    TARGET = None

# data set structure
class Dataset():
    location = {}
    size = 0
    availableTime = 0
    fileinfo = ""

# Scheduler same node listener
class schedulerThread(threading.Thread):
    def __init__(self, threadId, threadName):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.threadName = threadName
    def run(self):
        print "==> Staring thread ", self.threadName
        scheduler_listen()
        print "==> Exiting thread ", self.threadName

def scheduler_listen():
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((LISTEN_HOST, SCHEDULER_PORT))
        s.listen(10)
        while 1:
            conn, addr = s.accept()
            print "==> Connected by ", addr
            data = conn.recv(BUFSIZE)
            nodeDataset = pickle.loads(data)
            conn.close()
            if nodeDataset:
                # print "==> node type:", nodeDataset.MODE
                if nodeDataset.MODE == "SAME":
                    # print "==> Received from center for same node scheduling: ", nodeDataset.ID
                    # print "==> Received from center for same node scheduling: ", nodeDataset.MODE
                    # print "==> Received from center for same node scheduling: ", nodeDataset.SOURCE.size
                    # print "==> Received from center for same node scheduling: ", nodeDataset.TARGET.size
                    same_node_compose(nodeDataset)
                if nodeDataset.MODE == "DIFF":
                    # print "==> Received from center for diff node scheduling: ", nodeDataset.ID
                    # print "==> Received from center for diff node scheduling: ", nodeDataset.MODE
                    # print "==> Received from center for diff node scheduling: ", nodeDataset.SOURCE.size
                    # print "==> Received from center for diff node scheduling: ", nodeDataset.TARGET.size
                    Cons.CURRENT_NODE = nodeDataset.TARGET.location
                    Cons.TOTAL_SIZE = int(nodeDataset.SOURCE.size) + int(nodeDataset.TARGET.size)
                    Cons.COMPOSED_AVAILABLE_TIME = int(nodeDataset.SOURCE.availableTime) + int(nodeDataset.TARGET.availableTime)
                    # print "==> Test: ", Cons.CURRENT_NODE.keys()
                    #print "==> Test: " + "Current node" + str(Cons.CURRENT_NODE) + "; Total size: " + \
                          # str(Cons.TOTAL_SIZE) + "; new available time: " + str(Cons.COMPOSED_AVAILABLE_TIME)
                    print "==> Sending flag is: ", nodeDataset.SENDING_FLAG
                    if nodeDataset.SENDING_FLAG == "SOURCE":
                        file_transfer(nodeDataset)
    except socket.error as msg:
        print "==> XXX Receiving from center ", msg
        sys.exit(1)
    finally:
        s.close()

def same_node_compose(dataObj):
    print "==> Start compose on: ", dataObj.SOURCE.location
    composedDS = Dataset()
    composedDS.location = dataObj.SOURCE.location
    total_size = int(dataObj.SOURCE.size) + int(dataObj.TARGET.size)
    composedDS.size = dataBlockMin + (total_size - dataBlockMax) / (dataBlockMax - dataBlockMin)
    total_available_time = int(dataObj.SOURCE.availableTime) + int(dataObj.TARGET.availableTime)
    composedDS.availableTime = total_available_time
    composedDS.fileinfo = str(dataObj.SOURCE.location.keys()[0]) + "-data" + str(total_available_time)
    print "==> Composed Data: " + str(composedDS.location) + "," + str(composedDS.availableTime)
    print "==> Start generating new files..."
    createFileTimeStart = time.time()
    filename = composedDS.fileinfo + '.csv'
    print "==> Generated file name: ", filename
    filesize = int(composedDS.size) * 1024 * 1024
    print "==> Generated file size: ", filesize
    createFile('Files/' + filename, filesize)
    createFileTimeEnd = time.time()
    print "==> End creating file, time cost: " + str(createFileTimeEnd - createFileTimeStart)
    composition(total_size, Cons.COMPOSITION_MODE)
    composedDS_string = pickle.dumps(composedDS)
    Socket.send(SCHEDULING_CENTER, COMPOSER_PORT, composedDS_string)


# file transfer function
def file_transfer(dataForFileTransfer):
    # print "Starting file transfer..."
    sourceIP = dataForFileTransfer.SOURCE.location.values()[0]
    targetIP = dataForFileTransfer.TARGET.location.values()[0]
    # print "==> As source node, ip" + str(sourceIP) + ", start to transfer data set to target node " + str(targetIP)
    filepath = 'Files/' + str(dataForFileTransfer.SOURCE.fileinfo).replace("\n", "") + '.csv'
    print "==> File to be sent: ", filepath
    file_sender(targetIP, FILE_PORT, filepath)


# File sender
def file_sender(ipaddr, port, fpath):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ipaddr, port))
    except socket.error as msg:
        print "XXX Cannot connect to file server ==> ", msg
        sys.exit(1)

    # print s.recv(1024)

    while 1:
        filepath = fpath
        if os.path.isfile(filepath):
            fileinfo_size = struct.calcsize('128sl')
            fhead = struct.pack('128sl', os.path.basename(filepath), os.stat(filepath).st_size)
            s.send(fhead)
            print '==> client filepath: {0}'.format(filepath)

            fp = open(filepath, 'rb')
            while 1:
                data = fp.read(1024)
                if not data:
                    print '==> {0} file send over...'.format(filepath)
                    break
                s.send(data)
        s.close()
        break

# File receiver
class fileThread(threading.Thread):
    def __init__(self, threadId, threadName):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.threadName = threadName

    def run(self):
        print "==> Staring thread ", self.threadName
        # function in response thread
        socket_service()
        print "==> Exiting thread ", self.threadName

def socket_service():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('0.0.0.0', FILE_PORT))
        s.listen(10)
    except socket.error as msg:
        print msg
        sys.exit(1)
    while 1:
        conn, addr = s.accept()
        t = threading.Thread(target=deal_data, args=(conn, addr))
        t.start()

def deal_data(conn, addr):
    print '==> File receiving, accept source file from {0}'.format(addr)
    while 1:
        fileinfo_size = struct.calcsize('128sl')
        buf = conn.recv(fileinfo_size)
        if buf:
            filename, filesize = struct.unpack('128sl', buf)
            fn = filename.strip('\00')
            # new_filename = os.path.join('./Files/', str(time.time()).split(".")[0] + '-' + fn)
            new_filename = os.path.join('./Files/', fn)
            print '==> file new name is {0}, filesize if {1}'.format(new_filename, filesize)

            recvd_size = 0
            fp = open(new_filename, 'wb')
            print '==> Start receiving file...'

            while not recvd_size == filesize:
                if filesize - recvd_size > 1024:
                    data = conn.recv(1024)
                    recvd_size += len(data)
                else:
                    data = conn.recv(filesize - recvd_size)
                    recvd_size = filesize
                fp.write(data)
            fp.close()
            print '==> End receiving file...'
        conn.close()
        break
    # start to do composition
    compose()

# create file with assigned size
def createFile(filename, size):
    with open(filename, 'wb') as fd:
        fd.seek(size - 1)
        fd.write(b'\x00')

def compose():
    print "==> Start composing..."
    composedDS = Dataset()
    composedDS.location = Cons.CURRENT_NODE
    composedDS.size = dataBlockMin + (Cons.TOTAL_SIZE - dataBlockMax)/(dataBlockMax - dataBlockMin)
    composedDS.availableTime = Cons.COMPOSED_AVAILABLE_TIME
    print "==> Composed Dataset: " + str(composedDS.location) + "," + str(composedDS.size) + \
          "," + str(composedDS.availableTime)
    composedDS.fileinfo = str(Cons.CURRENT_NODE.keys()[0]) + '-data' + str(Cons.COMPOSED_AVAILABLE_TIME)
    print "==> Start generating new files..."
    createFileTimeStart = time.time()
    filename = composedDS.fileinfo + '.csv'
    print "==> Generated file name: ", filename
    filesize = int(composedDS.size) * 1000 * 1000
    print "==> Generated file size: ", filesize
    createFile('Files/' + filename, filesize)
    createFileTimeEnd = time.time()
    print "==> End creating file, time cost: " + str(createFileTimeEnd - createFileTimeStart)
    composition(Cons.TOTAL_SIZE, Cons.COMPOSITION_MODE)
    composedDS_string = pickle.dumps(composedDS)
    Socket.send(SCHEDULING_CENTER, COMPOSER_PORT, composedDS_string)

def composition(total_size, mode):
    # time complexity: n
    if mode == 1:
        # time1 = math.floor(Cons.COMPOSITION_CONSTANT * total_size)
        time1 = Cons.COMPOSITION_CONSTANT * total_size
        print "+++++++ Constant: ", Cons.COMPOSITION_CONSTANT
        print "+++++++ Total size: ", total_size
        print "+++++++ Mode 1: Sleep " + str(time1) + " s"
        time.sleep(time1)
    # time complexity: nlogn
    elif mode == 2:
        # time2 = math.floor(Cons.COMPOSITION_CONSTANT * total_size * math.log(total_size, 2))
        time2 = Cons.COMPOSITION_CONSTANT * total_size * math.log(total_size, 2)
        print "+++++++ Constant: ", Cons.COMPOSITION_CONSTANT
        print "+++++++ Total size: ", total_size
        print "+++++++ Mode 2: Sleep " + str(time2) + " s"
        time.sleep(time2)
    # time complexity: n^2
    elif mode == 3:
        # time3 = math.floor(Cons.COMPOSITION_CONSTANT * total_size * total_size)
        time3 = Cons.COMPOSITION_CONSTANT * total_size * total_size
        print "+++++++ Constant: ", Cons.COMPOSITION_CONSTANT
        print "+++++++ Total size: ", total_size
        print "+++++++ Mode 3: Sleep " + str(time3) + " s"
        time.sleep(time3)
    else:
        print "==> Exception: The mode of composition is wrong."


if __name__=='__main__':
    # Create file directory
    '''
    if os.path.exists("Files"):
        shutil.rmtree("Files")
        os.mkdir('Files')
    else:
        try:
            os.mkdir('Files')
        except Exception, e:
            print "==> Exception:", e
    '''

    # Thread for listening from same node scheduler
    try:
        schedulerThread = schedulerThread(5, "SCHEDULE_THREAD")
        schedulerThread.start()
    except Exception, e:
        print "==> Exception: ", e

    # Thread for file
    try:
        fileThread = fileThread(2, "FILE_THREAD")
        fileThread.start()
    except Exception, e:
        print "==> Exception: ", e