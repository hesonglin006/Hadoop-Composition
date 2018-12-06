class Dataset():
    location = {}
    size = 0
    availableTime = 0
    fileinfo = ""

class LoadConf():
    @classmethod
    def load(cls):
        dataList = []
        print "Start reading configuration file..."
        with open("./conf.txt", "r") as fd:
            lines = fd.readlines()
        for line in lines:
            print "==> Lines: ", line
            dataset = Dataset()
            dataset.location = {line.split(",")[0].split(":")[0]:line.split(",")[0].split(":")[1]}
            dataset.size = int(line.split(",")[1])
            dataset.availableTime = int(line.split(",")[2])
            dataset.fileinfo = str(line.split(":")[0]) + "-" + str(line.split(",")[3])
            dataList.append(dataset)
        # print "==> Before:", dataList[0].location
        # sort the list based on available time
        dataList.sort(cmp=None, key=lambda x:int(x.availableTime), reverse=False)
        return dataList