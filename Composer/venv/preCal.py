from Initialization_pre import LoadConf
import time

initial = []
group_list = []
temp_group = []
group_size = 4
block_min = 64
block_max = 128
result = []

layer3 = []

BANDWIDTH = [([0] * 12) for i in range(12)]
SCHEDULE_FLAG = True

class Dataset():
    location = {}
    size = 0
    availableTime = 0
    fileinfo = ""
    layer = 0

def compose(ds1, ds2):
    print "==>Test ds1: ", ds1.location.keys()[0]
    print "==>Test ds2: ", ds2.location.keys()[0]
    composedDS = Dataset()
    composedDS.availableTime = ds1.availableTime + ds2.availableTime
    total_size = ds1.size + ds2.size
    composedDS.size = block_min + (total_size - block_max)/(block_max - block_min)
    if ds1.location.keys()[0] == ds2.location.keys()[0]:
        composedDS.location = ds1.location
        composedDS.fileinfo = str(ds1.location.keys()[0]) + '-data' + str(composedDS.availableTime)
    else:
        # Pick the correct destination for transferring by formula: transfer_time = data_size / bandwidth
        bandwidthIndexOne = int(str(ds1.location.keys()[0]).replace("vp", ""))
        bandwidthIndexTwo = int(str(ds2.location.keys()[0]).replace("vp", ""))
        bandwidthOneToTwo = BANDWIDTH[bandwidthIndexOne - 1][bandwidthIndexTwo - 1]
        bandwidthTwoToOne = BANDWIDTH[bandwidthIndexTwo - 1][bandwidthIndexOne - 1]
        print "==> bandwidthOneToTwo: ", bandwidthOneToTwo
        print "==> bandwidthTwoToOne: ", bandwidthTwoToOne
        if (ds1.size / bandwidthOneToTwo) <= (ds2.size / bandwidthTwoToOne):
            composedDS.location = ds2.location
            composedDS.fileinfo = str(ds2.location.keys()[0]) + '-data' + str(composedDS.availableTime)
        else:
            composedDS.location = ds1.location
            composedDS.fileinfo = str(ds1.location.keys()[0]) + '-data' + str(composedDS.availableTime)
    print "==> Layer 2: "
    print "****** available Time: " + str(composedDS.availableTime) + ", size: " + str(composedDS.size) + ", fileinfo: " + str(composedDS.fileinfo)
    result.append(composedDS)
    return composedDS


def newsize():
    pass

if __name__=='__main__':
    initial = LoadConf.load()
    print initial
    for i in range(0, len(initial), 1):
        print str(initial[i].availableTime) + ", " + str(initial[i].size) + ", " + str(initial[i].fileinfo) + ", " + str(initial[i].location)

    # read bandwidth
    with open('bandwidth.txt', 'r') as fd:
        nodes = fd.readlines()
        print "==> nodes", nodes
        for i in range(0, int(nodes[0])):
            for j in range(0, int(nodes[0])):
                BANDWIDTH[i][j] = float(nodes[i + 1].split(" ")[j])
    print "==> Bandwidth: ", BANDWIDTH
    total_availabletime = 0
    if len(initial) != 0:
        print "Data sequence in initial List: "
        for i in range(0, len(initial), 1):
            total_availabletime += initial[i].availableTime
        print "==> total_availabletime: ", total_availabletime
        while len(initial) > 0:
            if SCHEDULE_FLAG:
                operatorOne = initial.pop(0)
                operatorTwo = initial.pop(0)
                print "==> operator One: ", operatorOne.availableTime
                print "==> operator Two: ", operatorTwo.availableTime

                layer2 = compose(operatorOne, operatorTwo)
                # insert into GL
                for i in range(0, len(initial)):
                    if layer2.availableTime <= initial[i].availableTime:
                        initial.insert(i, layer2)
                        break
                    elif i == len(initial) - 1:
                        initial.append(layer2)
                    else:
                        continue
                print "Updated..., length is" + str(len(initial)) + "new GL available time is: "
                for i in range(0, len(initial)):
                    print "==> ", initial[i].availableTime
                print "-------------------------------------------------------"
        print "Done"
    for i in range(0, len(result)):
        print "* " + str(result[i].availableTime) + ", size: " + str(result[i].size) + ", fileinfo: " + str(result[i].fileinfo)
