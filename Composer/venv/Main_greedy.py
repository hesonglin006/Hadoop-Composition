from __future__ import division
import time
import socket, pickle
import threading
import SocketServer
import sys
import pickle
import uuid
from Initialization import LoadConf
from SocketCom import Socket

BROADCAST_PORT = 37006
BUFSIZE = 4096
SCHEDULER_PORT = 27006
LISTEN_HOST = '0.0.0.0'
COMPOSER_PORT = 47006
globalList = []

class Var():
    SCHEDULING_FLAG = True
    startSchedulingTime = 0
    endSchedulingTime = 0
    BANDWIDTH = [([0] * 9) for i in range(9)]

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

# Thread for scheduler listener
class Main():
    class ComposerServerHan(SocketServer.BaseRequestHandler):
        def handle(self):
            try:
                self.data = self.request.recv(2048)
                if not self.data:
                    print("==> Listen for composer...\n")
                else:
                    print "==> Received from composer: ", self.data

            except Exception, e:
                print "XXX Exception: ", e

    def listen(self, *args):
        print ("==> Listening from composer on: " + str(args[0]) + " : " + str(args[1]))
        host = args[0]
        port = args[1]
        try:
            server = SocketServer.ThreadingTCPServer((host, port), self.ComposerServerHan)
            server.serve_forever()
        except Exception, e:
            print "XXX Exception: ", e

# Composer listener
class composerThread(threading.Thread):
    def __init__(self, threadId, threadName):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.threadName = threadName
    def run(self):
        print "==> Staring thread ", self.threadName
        # function in response thread
        composer_listen()
        print "==> Exiting thread ", self.threadName

def composer_listen():
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((LISTEN_HOST, COMPOSER_PORT))
        s.listen(10)
        while 1:
            conn, addr = s.accept()
            print "==> Connected by ", addr
            data = conn.recv(BUFSIZE)
            composedDataset = pickle.loads(data)
            conn.close()
            if composedDataset:
                print "==> composer received available time: ", composedDataset.availableTime
                print "==> composer received location: ", composedDataset.location
                print "==> composer received size: ", composedDataset.size
                print "==> composer received fileinfo: ", composedDataset.fileinfo
                print "==> Before inserting composed data set to GL, length is: ", len(globalList)
                if len(globalList) == 0:
                    print "Final result: "
                    print "************************************************"
                    print "*          Composition Result     " + str(composedDataset.availableTime) +   "           *"
                    print "************************************************"
                    break
                for i in range(0, len(globalList)):
                    if composedDataset.availableTime <= globalList[i].availableTime:
                        globalList.insert(i, composedDataset)
                        break
                    elif i == len(globalList) - 1:
                        globalList.append(composedDataset)
                    else:
                        continue
                print "==> After inserting, the global list length is: ", len(globalList)
                print "After inserting, the available time in new GL is: "
                for j in range(0, len(globalList)):
                    print "==> Updated available time: ", globalList[j].availableTime
                    # print "==> Updated available time type: ", type(globalList[j].availableTime)
                    print "==> Updated size: ", globalList[j].size
                    print "==> Updated location: ", globalList[j].location
                    print "==> Updated fileinfo: ", globalList[j].fileinfo
                Var.SCHEDULING_FLAG = True
    except socket.error as msg:
        print "==> XXX Receiving from composer ", msg
        sys.exit(1)
    finally:
        s.close()



if __name__=='__main__':
    dataBlockMin = 64
    dataBlockMax = 128
    # Load configuration file
    globalList = LoadConf.load()
    # Load bandwidth configuration
    with open('bandwidth.txt', 'r') as fd:
        nodes = fd.readlines()
        print "==> nodes", nodes
        for i in range(0, int(nodes[0])):
            for j in range(0, int(nodes[0])):
                Var.BANDWIDTH[i][j] = float(nodes[i + 1].split(" ")[j])
    print "==> Bandwidth: ", Var.BANDWIDTH
    # Thread for listening from composer and update global list
    try:
        composerThread = composerThread(3, "COMPOSER_THREAD")
        composerThread.start()
    except Exception, e:
        print "==> Exception: ", e

    if len(globalList) != 0:
        Var.startSchedulingTime = time.time()
        print "========> Start to scheduling, time is: ", str(Var.startSchedulingTime)
        print "Data sequence in Global List: "
        for i in range(0, len(globalList), 1):
            print globalList[i].availableTime
        while len(globalList) > 0:
            if Var.SCHEDULING_FLAG:
                operatorOne = globalList.pop(0)
                operatorTwo = globalList.pop(0)
                print "==> operator One: ", operatorOne.availableTime
                print "==> operator Two: ", operatorTwo.availableTime
                print "-------------------------------------------"
                print "Finding the scheduling IP addresses..."
                if operatorOne.location == operatorTwo.location:
                    payload = PayLoad()
                    targetIPS = operatorOne.location.values()[0]
                    schedulingIDS = uuid.uuid4()
                    payload.ID = schedulingIDS
                    payload.MODE = "SAME"
                    payload.SOURCE = operatorOne
                    payload.TARGET = operatorTwo
                    temp_result = pickle.dumps(payload)
                    print "==> Data locates on same node, notify node " + str(targetIPS) + " to compose..."
                    Socket.send(targetIPS, SCHEDULER_PORT, temp_result)
                    Var.SCHEDULING_FLAG = False
                else:
                    # Pick the correct destination for transferring by formula: transfer_time = data_size / bandwidth
                    bandwidthIndexOne = int(str(operatorOne.location.keys()[0]).replace("vp", ""))
                    bandwidthIndexTwo = int(str(operatorTwo.location.keys()[0]).replace("vp", ""))
                    bandwidthOneToTwo = Var.BANDWIDTH[bandwidthIndexOne - 1][bandwidthIndexTwo - 1]
                    bandwidthTwoToOne = Var.BANDWIDTH[bandwidthIndexTwo - 1][bandwidthIndexOne - 1]
                    print "==> bandwidthOneToTwo: ", bandwidthOneToTwo
                    print "==> bandwidthTwoToOne: ", bandwidthTwoToOne

                    if (int(operatorOne.size) / bandwidthOneToTwo) <= (int(operatorTwo.size) / bandwidthTwoToOne):
                        source = operatorOne
                        target = operatorTwo
                    else:
                        source = operatorTwo
                        target = operatorOne
                    sourceIP = source.location.values()[0]
                    targetIP = target.location.values()[0]
                    print "==> The source IP is: ", sourceIP
                    print "==> The target IP is: ", targetIP
                    print "-------------------------------------------"
                    print "Start scheduling..."
                    schedulingID = uuid.uuid4()
                    print "Scheduling ID is: ", schedulingID
                    # data format: [schedulingID, "SOURCE", source, target]
                    payloadDS = PayLoad()
                    payloadDS.ID = schedulingID
                    payloadDS.MODE = "DIFF"
                    payloadDS.SENDING_FLAG = "SOURCE"
                    payloadDS.SOURCE = source
                    payloadDS.TARGET = target
                    payloadDT = PayLoad()
                    payloadDT.ID = schedulingID
                    payloadDT.MODE = "DIFF"
                    payloadDT.SENDING_FLAG = "TARGET"
                    payloadDT.SOURCE = source
                    payloadDT.TARGET = target
                    # targetPayload.append(time.time())
                    temp_resultDS = pickle.dumps(payloadDS)
                    temp_resultDT = pickle.dumps(payloadDT)
                    Socket.send(sourceIP, SCHEDULER_PORT, temp_resultDS)
                    Socket.send(targetIP, SCHEDULER_PORT, temp_resultDT)
                    Var.SCHEDULING_FLAG = False
            else:
                # time.sleep(1)
                # print "==> Waiting to do next composition"
                pass
        Var.endSchedulingTime = time.time()
        print "======> End Scheduling, time is: ", str(Var.endSchedulingTime)
        print "************************************************"
        print "*         Time Cost     " + str(Var.endSchedulingTime - Var.startSchedulingTime) + " s        *"
        print "************************************************"
    else:
        print "==> The gloal list is empty!"
        exit(0)