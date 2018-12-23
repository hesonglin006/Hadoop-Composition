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
from pathlib import Path

from SocketCom import Socket

BUFSIZE = 4096
LISTEN_HOST = '0.0.0.0'
FILE_PORT = 27008
COMPOSER_PORT = 47006
SCHEDULING_CENTER = 'xx.xx.xx.xx'
SCHEDULER_PORT = 27006


class Cons():
    # composition mode: 1, n; 2, nlogn; 3, n^2.
    COMPOSITION_MODE = int(sys.argv[1])
    COMPOSITION_CONSTANT = 1
    TOTAL_SIZE = 0
    RECEIVING_QUEUE = []
    COMPOSED_AVAILABLE_TIME = 0
    COMPOSITION_TIME = 2
    CURRENT_NODE = {}
    DATA_BLOCK_MIN = 64
    DATA_BLOCK_MAX = 128
    FILE_COUNTER = {}
    FILE_COUNTER_FLAG = False
    GROUP_SIZE = 4
    lock = threading.Lock()

class PayLoad():
    ID = ""
    MODE = ""
    SENDING_FLAG = ""
    SOURCE = None
    SOURCENODE = None
    TARGET = None

# data set structure
class Dataset():
    location = {}
    size = 0
    availableTime = 0
    fileinfo = ""
    layer = 0


# File Counter Thread
class fileCounterThread(threading.Thread):
    def __init__(self, threadId, threadName):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.threadName = threadName
    def run(self):
        print "==> Staring thread ", self.threadName
        # function in response thread
        file_counter_check()
        print "==> Exiting thread ", self.threadName

def file_counter_check():
    # check if target received transferred data
    # {ID:[counter, [source nodes], groupItem]}
    while 1:
        for (d, x) in Cons.FILE_COUNTER.items():
            for i in range(len(x[1])):
                print "==> x: ", x
                if Path("./Files/" + x[1][i]):
                    x[0] += 1
                    # x[1].remove(x[1][i])
                    x[1][i] = 'Done'
            if x[0] == len(x[1]):
                print "All files are here, counter: ", x[0]
                compose(d, x[2])
                Cons.FILE_COUNTER.pop(d)
            # Cons.FILE_COUNTER_FLAG = False

# Scheduler listener
class schedulerThread(threading.Thread):
    def __init__(self, threadId, threadName):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.threadName = threadName
    def run(self):
        print "==> Staring thread ", self.threadName
        # function in response thread
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
                print "==> Received scheduling task, current length of Task Queue: ", len(Cons.RECEIVING_QUEUE)
                Cons.RECEIVING_QUEUE.append(nodeDataset)
    except socket.error as msg:
        print "==> XXX Receiving from center ", msg
        sys.exit(1)
    finally:
        s.close()


# file transfer function
def file_transfer(dataForFileTransfer):
    # print "Starting file transfer..."
    # print "==> As source node, start to transfer data set to target node..."
    # sourceIP = dataForFileTransfer[2].strip().lstrip("\'").rstrip("\'")
    targetIP = dataForFileTransfer.TARGET.location.values()[0]
    print "==> Target IP is: ", targetIP
    print "==> fileinfo: ", dataForFileTransfer.SOURCENODE.fileinfo
    filepath = 'Files/' + str(dataForFileTransfer.SOURCENODE.fileinfo).replace("\n","") + '.csv'
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
    # compose()
    Cons.FILE_COUNTER_FLAG = True

# create file with assigned size
def createFile(filename, size):
    with open(filename, 'wb') as fd:
        fd.seek(size - 1)
        fd.write(b'\x00')

def compose(ID, dataObj):
    print "==> Start composing...", ID
    total_size = 0
    composed_available_time = 0
    composedDS = Dataset()
    composedDS.location = dataObj.TARGET.location
    for i in range(0, len(dataObj.SOURCE)):
        total_size += dataObj.SOURCE[i].size
    composedDS.size = Cons.DATA_BLOCK_MIN + (total_size - Cons.DATA_BLOCK_MAX)/(Cons.DATA_BLOCK_MAX - Cons.DATA_BLOCK_MIN)
    for i in range(0, len(dataObj.SOURCE)):
        composed_available_time += dataObj.SOURCE[i].availableTime
    composedDS.availableTime = composed_available_time
    composedDS.fileinfo = dataObj.TARGET.location.keys()[0] + "-data" + str(composed_available_time)
    print "==> Start generating new files..."
    createFileTimeStart = time.time()
    filename = composedDS.fileinfo + '.csv'
    print "==> Generated file name: ", filename
    filesize = int(composedDS.size) * 1024 * 1024
    print "==> Generated file size: ", filesize
    createFile('Files/' + filename, filesize)
    createFileTimeEnd = time.time()
    print "==> End creating file, time cost: " + str(createFileTimeEnd - createFileTimeStart)
    print "==> Composed Dataset: " + str(composedDS.location) + "," + str(composedDS.size) + "," + str(composedDS.availableTime)
    composition(total_size, Cons.COMPOSITION_MODE)
    composedDS_string = pickle.dumps(composedDS)
    Socket.send(SCHEDULING_CENTER, COMPOSER_PORT, composedDS_string)


def same_node_compose(dataObj):
    total_size = 0
    total_available_time = 0
    print "==> Start compose on: ", dataObj.SOURCE[0].location
    composedDS = Dataset()
    composedDS.location = dataObj.SOURCE[0].location
    for i in range(0, len(dataObj.SOURCE)):
        total_size += dataObj.SOURCE[i].size
    composedDS.size = Cons.DATA_BLOCK_MIN + (total_size - Cons.DATA_BLOCK_MAX)/(Cons.DATA_BLOCK_MAX - Cons.DATA_BLOCK_MIN)
    for j in range(0, len(dataObj.SOURCE)):
        print "==>Debug: Before composing, the available time are: ", dataObj.SOURCE[j].availableTime
        total_available_time += dataObj.SOURCE[j].availableTime
    composedDS.availableTime = total_available_time
    composedDS.fileinfo = str(dataObj.SOURCE[0].location.keys()[0]) + "-data" + str(total_available_time)
    print "==> Start generating new files..."
    createFileTimeStart = time.time()
    filename = composedDS.fileinfo + '.csv'
    print "==> Generated file name: ", filename
    filesize = int(composedDS.size) * 1024 * 1024
    print "==> Generated file size: ", filesize
    createFile('Files/' + filename, filesize)
    createFileTimeEnd = time.time()
    print "==> End creating file, time cost: " + str(createFileTimeEnd - createFileTimeStart)
    # composedDS.layer = dataObj.SOURCE[0].layer + 1
    # print "==> Composed Data: " + str(composedDS.location) + "," + str(composedDS.availableTime)
    # print "==> Composing...", time.time()
    composition(total_size, Cons.COMPOSITION_MODE)
    print "Finished composing, time cost: ", time.time()
    composedDS_string = pickle.dumps(composedDS)
    Socket.send(SCHEDULING_CENTER, COMPOSER_PORT, composedDS_string)

def composition(total_size, mode):
    # time complexity: n
    if mode == 1:
        time1 = Cons.COMPOSITION_CONSTANT * (total_size / 64)
        print "+++++++ Constant: ", Cons.COMPOSITION_CONSTANT
        print "+++++++ Total size: ", (total_size / 64)
        print "+++++++ Mode 1: Sleep " + str(time1) + " s"
        time.sleep(time1)
    # time complexity: nlogn
    elif mode == 2:
        time2 = Cons.COMPOSITION_CONSTANT * (total_size / 64) * math.log((total_size / 64), 2)
        print "+++++++ Constant: ", Cons.COMPOSITION_CONSTANT
        print "+++++++ Total size: ", total_size / 64
        print "+++++++ Mode 2: Sleep " + str(time2) + " s"
        time.sleep(time2)
    # time complexity: n^2
    elif mode == 3:
        time3 = Cons.COMPOSITION_CONSTANT * (total_size / 64) * (total_size / 64)
        print "+++++++ Constant: ", Cons.COMPOSITION_CONSTANT
        print "+++++++ Total size: ", total_size / 64
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
    # Thread for listening scheduler
    try:
        schedulerThread = schedulerThread(5, "SCHEDULER_THREAD")
        schedulerThread.start()
    except Exception, e:
        print "==> Exception: ", e

    # Thread for checking File Counter
    try:
        fileCounterThread = fileCounterThread(7, "FILE_COUNTER_THREAD")
        fileCounterThread.start()
    except Exception, e:
        print "==> Exception: ", e

    # Thread for file
    try:
        fileThread = fileThread(2, "FILE_THREAD")
        fileThread.start()
    except Exception, e:
        print "==> Exception: ", e

    while 1:
        if len(Cons.RECEIVING_QUEUE) > 0:
            # time.sleep(1)
            nodeDataset = Cons.RECEIVING_QUEUE.pop(0)
            print "==> Start to handle data set ", nodeDataset.ID
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
                # find the target data set object based on target node name
                for i in range(0, len(nodeDataset.SOURCE)):
                    if nodeDataset.TARGET == nodeDataset.SOURCE[i].location.keys()[0]:
                        nodeDataset.TARGET = nodeDataset.SOURCE[i]
                        break
                # print "==> Test: ", Cons.CURRENT_NODE.keys()
                # print "==> Test: " + "Current node" + str(Cons.CURRENT_NODE) + "; Total size: " + \
                # str(Cons.TOTAL_SIZE) + "; new available time: " + str(Cons.COMPOSED_AVAILABLE_TIME)
                print "==> Sending flag is: ", nodeDataset.SENDING_FLAG
                # print "==> Node Dataset TARGET: ", nodeDataset.TARGET
                if nodeDataset.SENDING_FLAG == "TARGET":
                    sourceNodes = []
                    for i in range(0, len(nodeDataset.SOURCE)):
                        if nodeDataset.TARGET.location.keys()[0] == nodeDataset.SOURCE[i].location.keys()[0]:
                            continue
                        else:
                            sourceNodes.append(str(nodeDataset.SOURCE[i].fileinfo).replace('\n', '') + '.csv')
                    with Cons.lock:
                        # print "==> Updating FILE COUNTER..., sourceNodes: " + str(sourceNodes) + ", " + str(nodeDataset.ID) + ", " + str(nodeDataset.TARGET.location)
                        Cons.FILE_COUNTER.update({nodeDataset.ID: [0, sourceNodes, nodeDataset]})
                    print "==> FILE COUNTER Status: ", Cons.FILE_COUNTER
                if nodeDataset.SENDING_FLAG == "SOURCE":
                    file_transfer(nodeDataset)
