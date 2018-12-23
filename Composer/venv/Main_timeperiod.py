from __future__ import division
import time
import socket, pickle
import threading
import SocketServer
import sys
import threading
import random
import math
import uuid
from Initialization import LoadConf
from SocketCom import Socket

# This is the time period based algorithm

BROADCAST_PORT = 37006
BUFSIZE = 4096
SCHEDULER_PORT = 27006
LISTEN_HOST = '0.0.0.0'
COMPOSER_PORT = 47006
globalList = []
nodeinfo = {'vp0':'xx.xx.xx.xx', 'vp1':'xx.xx.xx.xx', 'vp2':'xx.xx.xx.xx', 'vp3':'xx.xx.xx.xx', 'vp4':'xx.xx.xx.xx', 'vp5':'xx.xx.xx.xx', 'vp6':'xx.xx.xx.xx', 'vp7':'xx.xx.xx.xx', 'vp8':'xx.xx.xx.xx'}

class Var():
    STOP_FLAG = 0
    SCHEDULING_FLAG = False
    startSchedulingTime = 0
    endSchedulingTime = 0
    GROUP_SIZE = 4
    DEALED_INDEX = 0
    DATA_BLOCK_MIN = 64
    DATA_BLOCK_MAX = 128
    MAX_GROUP_SIZE = 5
    MIN_GROUP_SIZE = 2
    BANDWIDTH = [([0] * 9) for i in range(9)]
    TIMER_FLAG = True
    TIMER_TIME = 3
    lock = threading.Lock()

class PayLoad():
    ID = ""
    MODE = ""
    SENDING_FLAG = ""
    SOURCE = None
    SOURCENODE = None
    TARGET = None

class Group():
    id = 0
    datasets = []

# data set structure
class Dataset():
    location = {}
    size = 0
    availableTime = 0
    fileinfo = ""
    layer = 0

def pickMajority(input_list):
    d = dict()
    for element in input_list:
        if d.has_key(element):
            d[element] += 1
        else:
            d[element] = 1
    # print "d ===> ", d
    return d

def get_keys_on_value(d, value):
    return [k for k, v in d.items() if v == value]

# timer function
def func_timer(arg):
    # print "Timer finished, start to group, the handling length is: " + str(arg) + ", while the latest length is: " + str(len(globalList))
    with Var.lock:
        Var.TIMER_FLAG = True
        global_list_length = len(globalList)
        if global_list_length == 0 or global_list_length > Var.GROUP_SIZE or global_list_length == 1:
            pass
        else:
            group_temp = []
            for i in range(0, global_list_length):
                group_temp.append(globalList.pop(0))
            # create new thread to run group processing
            try:
                t = threading.Thread(target=groupHandling, args=(group_temp, random.randrange(50000, 60000, 100)))
                t.start()
            except Exception, e:
                print "==> Exception: ", e
        Var.TIMER_FLAG = False

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
                # print "==> composer received location: ", composedDataset.location
                # print "==> composer received size: ", composedDataset.size
                # print "==> composer received fileinfo: ", composedDataset.fileinfo
                # print "==> composer received layer: ", composedDataset.layer
                with Var.lock:
                    print "==> Before inserting composed data set to GL, length is: ", len(globalList)
                    if len(globalList) == 0:
                        # print "Composed available time result: "
                        # print "************************************************"
                        # print "*          Composition Result     " + str(composedDataset.availableTime) +   "           *"
                        # print "************************************************"
                        globalList.append(composedDataset)
                        # break
                    else:
                        for i in range(0, len(globalList)):
                            if composedDataset.availableTime <= globalList[i].availableTime:
                                globalList.insert(i, composedDataset)
                                break
                            elif i == len(globalList) - 1:
                                globalList.append(composedDataset)
                            else:
                                continue
                    print "==> After inserting, the global list length is: ", len(globalList)
                    print "==> Current group size is: ", len(globalList)
                    print "After inserting, the available time in new GL is: "
                    for j in range(0, len(globalList)):
                        print "==> Updated available time: ", globalList[j].availableTime
                        # print "==> Updated available time type: ", type(globalList[j].availableTime)
                        # print "==> Updated size: ", globalList[j].size
                        # print "==> Updated location: ", globalList[j].location
    except socket.error as msg:
        print "==> XXX Receiving from composer ", msg
        sys.exit(1)
    finally:
        s.close()


# Thread body for handling each group
def groupHandling(groupItem, threadID):
    # print "==> Start to handle groups in thread-" + str(threadID) + "\n"
    print "==> groupItem length: ", len(groupItem)
    temp_group_locations = []
    temp_group_locations_no_dup = []
    schedule_flag = False
    group_max_size_temp = []
    pairs = []
    majorityNode = ''
    # nodeinfo = {}
    involvedNodes = []

    group_length = len(groupItem)
    # get node info
    print "\n==> nodeinfo: ", nodeinfo
    # check the location of the data sets
    for i in range(0, group_length):
        temp_group_locations.append(groupItem[i].location.keys()[0])
    # no duplicated elements in the list, otherwise data will be sent repeatedly to source or target node
    for i in range(0, len(temp_group_locations)):
        if temp_group_locations[i] in temp_group_locations_no_dup:
            continue
        else:
            temp_group_locations_no_dup.append(temp_group_locations[i])
    for i in range(0, len(temp_group_locations)):
        if i == 0:
            pairs.append({int(temp_group_locations[i].replace("vp", "")): temp_group_locations[i]})
        else:
            temp = []
            for j in range(0, len(pairs)):
                temp.append(pairs[j].values()[0])
            if temp_group_locations[i] in temp:
                break
            else:
                pairs.append({int(temp_group_locations[i].replace("vp", "")): temp_group_locations[i]})
    print "==> temp_group_locations: " + str(temp_group_locations) + '\n'
    print "==> temp group_locations no repeat: " + str(temp_group_locations_no_dup) + '\n'
    # statistic the location
    locations = []
    for i in range(0, len(temp_group_locations)):
        locations.append(int(temp_group_locations[i].replace("vp", "")))
    print "==> locations: ", locations

    statisticLocation = pickMajority(locations)
    print "==> Statistic: ", statisticLocation
    print "==> Pairs: ", pairs
    maxvalue = []
    for (d, x) in statisticLocation.items():
        maxvalue.append(x)
    majority = max(maxvalue)
    for i in range(0, len(pairs)):
        if pairs[i].keys()[0] == get_keys_on_value(statisticLocation, majority)[0]:
            majorityNode = pairs[i].values()[0]
        else:
            majorityNode = pairs[0].values()[0]
    # eg., majorityNode: "vp1"
    print "==> majorityNode: ", majorityNode

    # if the data set locate in the same node, compose on this node, no transferring
    location_root = temp_group_locations[0]
    for j in range(1, len(temp_group_locations)):
        if temp_group_locations[j] != location_root:
            schedule_flag = True
            break
    print "==> schedule_flag: ", schedule_flag
    if schedule_flag:
        # print "data sets locate on different nodes, start scheduling based on data size...\n"
        # if most data sets locate on the same node, use that node as target to reduce transfer time
        targetIP = ''
        sumBandwidth = 0
        sumBandwidthList = []
        sumBandwidthListKey = {}
        # most data locate on same node
        if majority >= len(temp_group_locations)/2:
            print "==> most data on same node..."
            targetIP = nodeinfo[majorityNode]
            print "==> targetIP (most): ", targetIP
            print "==> Start scheduling..."
            schedulingID = uuid.uuid4()
            for i in range(0, len(temp_group_locations_no_dup)):
                if temp_group_locations_no_dup[i] == majorityNode:
                    # target node
                    payloadDT = PayLoad()
                    payloadDT.ID = schedulingID
                    payloadDT.MODE = "DIFF"
                    payloadDT.SENDING_FLAG = "TARGET"
                    payloadDT.SOURCE = groupItem
                    # payloadDT.SOURCENODE = groupItem[i]
                    payloadDT.TARGET = majorityNode
                    temp_resultDT = pickle.dumps(payloadDT)
                    print "==> Send to target node: ", targetIP
                    Socket.send(targetIP, SCHEDULER_PORT, temp_resultDT)
                else:
                    # source node
                    payloadDS = PayLoad()
                    payloadDS.ID = schedulingID
                    payloadDS.MODE = "DIFF"
                    payloadDS.SENDING_FLAG = "SOURCE"
                    payloadDS.SOURCE = groupItem
                    payloadDS.SOURCENODE = groupItem[i]
                    payloadDS.TARGET = majorityNode
                    temp_resultDS = pickle.dumps(payloadDS)
                    print "==> send to source node: ", groupItem[i].location
                    # Socket.send(groupItem[i].location.values()[0], SCHEDULER_PORT, temp_resultDS)
                    Socket.send(nodeinfo[temp_group_locations_no_dup[i]], SCHEDULER_PORT, temp_resultDS)
        else:
            print "==> no most data locate on same node..."
            # check the transfer time
            for (d, x) in statisticLocation.items():
                involvedNodes.append(d)
            print "==> involved nodes: ", involvedNodes
            for i in range(0, len(involvedNodes)):
                for j in range(0, len(involvedNodes)):
                    if j == i:
                        continue
                    sumBandwidth += float('%.2f' % Var.BANDWIDTH[involvedNodes[j]][involvedNodes[i]])
                sumBandwidthListKey.update({involvedNodes[i]: float('%.2f' % sumBandwidth)})
                sumBandwidthList.append(float('%.2f' % sumBandwidth))
                sumBandwidth = 0
            print "==> sumBandwidthList: ", sumBandwidthList
            print "==> sumBandwidthListKey: ", sumBandwidthListKey
            # find the minimum average transfer time for each node
            minRouter = max(sumBandwidthList)
            target = get_keys_on_value(sumBandwidthListKey, minRouter)
            targetNode = 'vp' + str(target[0])
            print "==> targetNode: ", targetNode
            targetIP = nodeinfo[targetNode]
            print "==> targetIP (non most): ", targetIP
            schedulingID = uuid.uuid4()
            for i in range(0, len(temp_group_locations_no_dup)):
                if temp_group_locations_no_dup[i] == targetNode:
                    # target node
                    payloadDT = PayLoad()
                    payloadDT.ID = schedulingID
                    payloadDT.MODE = "DIFF"
                    payloadDT.SENDING_FLAG = "TARGET"
                    payloadDT.SOURCE = groupItem
                    # payloadDT.SOURCENODE = groupItem[i]
                    payloadDT.TARGET = targetNode
                    temp_resultDT = pickle.dumps(payloadDT)
                    print "==> Send to target node: ", targetIP
                    Socket.send(targetIP, SCHEDULER_PORT, temp_resultDT)
                else:
                    # source node
                    payloadDS = PayLoad()
                    payloadDS.ID = schedulingID
                    payloadDS.MODE = "DIFF"
                    payloadDS.SENDING_FLAG = "SOURCE"
                    payloadDS.SOURCE = groupItem
                    payloadDS.SOURCENODE = groupItem[i]
                    payloadDS.TARGET = targetNode
                    temp_resultDS = pickle.dumps(payloadDS)
                    print "==> send to source node: ", groupItem[i].location
                    Socket.send(nodeinfo[temp_group_locations_no_dup[i]], SCHEDULER_PORT, temp_resultDS)
    else:
        print "data sets locate on the same node, start scheduling..."
        # check data sets size
        for sizeItem in range(0, len(groupItem)):
            group_max_size_temp.append(groupItem[sizeItem].size)
        group_max_size = max(group_max_size_temp)
        # find the location with smallest data set
        targetIP = groupItem[group_max_size_temp.index(group_max_size)].location.values()[0]
        print "==> Target IP for scheduling is: ", targetIP
        schedulingID = uuid.uuid4()
        targetPayload = PayLoad()
        targetPayload.ID = schedulingID
        targetPayload.MODE = "SAME"
        targetPayload.SENDING_FLAG = ""
        targetPayload.SOURCE = groupItem
        targetObj = pickle.dumps(targetPayload)
        Socket.send(targetIP, SCHEDULER_PORT, targetObj)

if __name__=='__main__':
    dataBlockMin = 64
    dataBlockMax = 128
    targetLocation = ''
    globalList = LoadConf.load()
    availableGroups = 0

    # Thread for listening from composer and update global list
    try:
        composerThread = composerThread(666, "COMPOSER_THREAD")
        composerThread.start()
    except Exception, e:
        print "==> Exception: ", e

    # Load bandwidth configuration
    with open('bandwidth.txt', 'r') as fd:
        nodes = fd.readlines()
        for i in range(0, int(nodes[0])):
            for j in range(0, int(nodes[0])):
                Var.BANDWIDTH[i][j] = float(nodes[i + 1].split(" ")[j])
    print "==> Bandwidth: ", Var.BANDWIDTH

    print "Global list length: ", len(globalList)
    if len(globalList) != 0:
        groupList = []
        temp_group = []
        for i in range(0, len(globalList)):
            Var.STOP_FLAG += globalList[i].availableTime
        print "==> STOP FLAG: ", Var.STOP_FLAG
        Var.startSchedulingTime = time.time()
        print "========> Start to scheduling, time is: ", str(Var.startSchedulingTime)
        print "Data sequence in Global List: "
        for i in range(0, len(globalList), 1):
            print str(globalList[i].availableTime) + ", " + str(globalList[i].location) + ", " + str(globalList[i].size)
        # Generate the available groups
        availableGroups = len(globalList) // Var.GROUP_SIZE
        Var.DEALED_INDEX = availableGroups * Var.GROUP_SIZE
        print "==> availableGroups: ", availableGroups
        for i in range(0, availableGroups * Var.GROUP_SIZE):
            if (i != 0) & (i % Var.GROUP_SIZE == 0):
                groupList.append(temp_group)
                temp_group = []
                # temp_group.append(globalList[i])
            temp_group.append(globalList.pop(0))
            if i == (availableGroups * Var.GROUP_SIZE-1):
                # temp_group.append(globalList[i])
                groupList.append(temp_group)
        print "==> groupList length: ", len(groupList)
        print "After grouping, the length of globalList is: ", len(globalList)
        # create threads to run group processing in parallel
        try:
            for i in range(0, availableGroups):
                t = threading.Thread(target=groupHandling, args=(groupList[i],i))
                t.start()
        except Exception, e:
            print "==> Exception: ", e
        # while len(globalList) > 0:
        while 1:
            # time period based algorithm
            time.sleep(Var.TIMER_TIME)
            length = len(globalList)
            if length > 0:
                if length == 1:
                    if (length == 1) & (globalList[0].availableTime >= Var.STOP_FLAG):
                        # the final composition result in GL
                        print "Finished Composition..."
                        break
                else:
                    # group all of them within this time period
                    print "==> Grouping data sets within this time period..."
                    temp_group = []
                    with Var.lock:
                        for i in range(0, length):
                            temp_group.append(globalList.pop(0))
                        try:
                            tl = threading.Thread(target=groupHandling, args=(temp_group, random.randrange(30000, 40000, 100)))
                            tl.start()
                        except Exception, e:
                            print "==> Exception: ", e
            else:
                pass
        Var.endSchedulingTime = time.time()
        print "======> End Scheduling, time is: ", str(Var.endSchedulingTime)
        print "************************************************"
        print "*         Time Cost     " + str(Var.endSchedulingTime - Var.startSchedulingTime) + " s        *"
        print "************************************************"
        exit(0)
    else:
        print "==> The gloal list is empty!"
        exit(0)