# your Python code to implement the features could be placed here
# note that you may use any language, there is no preference towards Python
import os
import re
import operator
import time
import datetime
import bisect

fname = os.path.realpath("./fansite-analytics-challenge//log_input/log_input.txt")
feature1_out = os.path.realpath("./fansite-analytics-challenge/log_output/hosts.txt")
feature2_out = os.path.realpath("./fansite-analytics-challenge/log_output/resources.txt")
feature3_out = os.path.realpath("./fansite-analytics-challenge/log_output/hours.txt")
feature4_out = os.path.realpath("./fansite-analytics-challenge/log_output/blocked.txt")

def past_graph():
    with open(fname) as fp:
        for line in fp:
            try:
                components = line.strip().split(" ")
                bigArray.append(components)
                host = re.search(r'([\w.-]+[.]*[\w.-])', line)
                resource = re.search(r'\"(.+?)\"', line)
                dateTimeStamp = re.search(r'\[(.+?)\]', line)
                http = components[-2].strip()
                bytes = components[-1].strip()

                # Feature 1 graph
                if host and http=='200':
                    mHost = str(host.group())
                    graph.setdefault(mHost, 0)
                    graph[mHost] += 1

                # Feature 2 graph2
                if resource:
                    try:
                        mResource = str(resource.group()).split()[1].strip()
                        if mResource != "/" and isinstance(int(bytes), int):
                            graph2.setdefault(mResource, 0)
                            graph2[mResource] += int(bytes)
                    except:
                        continue

                # Feature 3 timeArray to store timestamp for each event
                if dateTimeStamp:
                    try:
                        mDateTimeStamp = dateTimeStamp.group()
                        mTime = str(mDateTimeStamp).split(" ")[-2].strip()
                        mTimestamp = time.mktime(datetime.datetime.strptime(mTime, "[%d/%b/%Y:%H:%M:%S").timetuple())
                        timeArray.append(mTimestamp)
                    except:
                        continue
            except ValueError:
                    continue

# Function to find top 10 entries from given dictionary and write to destination file
def feature(graph,filename):
    try:
        output = sorted(graph.iteritems(), key=operator.itemgetter(1), reverse=True)[:10]
        with open(filename,'w') as f:
            for items in output:
                out = str(items[0])+","+str(items[1])
                print >> f, out
        f.close()
    except:
        return 0

# Function to convert timestamp to Date-Time-TimeZone Format
def getDateTime(timestamp):
    local_time = time.localtime(timestamp)
    dateTimeFormat = time.strftime("%d/%b/%Y:%H:%M:%S -0400", local_time)
    return dateTimeFormat

# Function to return index of right most occurrence of timestamp in timeArray
def getIndex(arr,value):
    i = bisect.bisect_right(arr, value)
    if i:
        return i-1
    raise ValueError

# Function for Feature 3
def feature3(graph3, timeList, block):
    sortItems = sorted(timeList)
    start = sortItems[0]
    end = sortItems[-1]
    while start <= end:
        previousKey = start
        key = getDateTime(previousKey)
        graph3[key] = 0
        jStart = bisect.bisect_left(sortItems,previousKey)
        jEnd = getIndex(sortItems, previousKey+block)
        graph3[key] = jEnd - jStart + 1
        start += 1

# Function for Feature 4
def feature4(blockedHosts,timeArray,bigArray,smallWindow, bigWindow):
    blocks_file = open(feature4_out,'w')
    i = 0
    while i < len(timeArray):
        httpCode = bigArray[i][-2]
        hostAddress = bigArray[i][0]

        if httpCode == '200' and hostAddress not in blockedHosts:
            i += 1
            continue

        elif httpCode == '200' and hostAddress in blockedHosts and (timeArray[i] - blockedHosts[hostAddress][0]) <= bigWindow:
            if (timeArray[i] - blockedHosts[hostAddress][0]) <= smallWindow and blockedHosts[hostAddress][1] < 3:
                del blockedHosts[hostAddress]
            elif blockedHosts[hostAddress][1] == 3:
                blocks_file.write(' '.join(bigArray[i]) + '\n')
            i += 1
            continue

        elif hostAddress in blockedHosts and (timeArray[i] - blockedHosts[hostAddress][0]) <= bigWindow and blockedHosts[hostAddress][1] == 3:
            blocks_file.write(' '.join(bigArray[i]) + '\n')
            i += 1
            continue

        elif httpCode == '200' and hostAddress in blockedHosts and (timeArray[i]-blockedHosts[hostAddress][0]) > smallWindow:
            del blockedHosts[hostAddress]
            i += 1
            continue

        elif httpCode != '200' and hostAddress in blockedHosts and (timeArray[i]-blockedHosts[hostAddress][0]) > smallWindow:
            del blockedHosts[hostAddress]
            blockedHosts[hostAddress] = [timeArray[i], 1]
            i += 1
            continue

        elif httpCode != '200' and hostAddress not in blockedHosts:
            blockedHosts[hostAddress] = [timeArray[i], 1]
            i += 1
            continue

        elif httpCode != '200' and hostAddress in blockedHosts and (timeArray[i]-blockedHosts[hostAddress][0]) <= smallWindow:
            blockCount = blockedHosts[hostAddress][1] + 1
            blockedHosts[hostAddress][1] = blockCount
            i += 1
            continue
        i += 1

if __name__ == '__main__':
    graph = {}                          # Dictionary for feature 1
    graph2={}                           # Dictionary for feature 2
    graph3 = {}                         # Dictionary for feature 3
    blockedHosts = {}                   # Dictionary for feature 4
    timeArray = []                      # Array to store timestamp of each event
    bigArray = []                       # Array to store all components of line
    smallBlock = 20                     # 20 seconds window
    bigBlock = 300                      # 5 minutes window
    superBlock = 3600                   # 60 minutes window

    # Read data from file and construct dictionary
    start = time.time()
    past_graph()
    end = time.time()
    print "Graph contruction: %.2f seconds"%(end-start)

    # Feature 1
    start = time.time()
    feature(graph,feature1_out)
    end = time.time()
    print "Feature 1 time: %.2f seconds"%(end-start)

    # Feature 2
    start = time.time()
    feature(graph2,feature2_out)
    end = time.time()
    print "Feature 2 time: %.2f seconds" % (end - start)

    # Feature 3
    start = time.time()
    feature3(graph3, timeArray, superBlock)
    end = time.time()
    print "Feature 3 time: %.2f seconds" % (end - start)
    feature(graph3,feature3_out)

    # Feature 4
    start = time.time()
    feature4(blockedHosts,timeArray,bigArray,smallBlock, bigBlock)
    end = time.time()
    print "Feature 4 time: %.2f seconds" % (end - start)

'''
Graph contruction: 159.37 seconds
Feature 1 time: 1.55 seconds
Feature 2 time: 0.05 seconds
Feature 3 time: 11.59 seconds
Feature 4 time: 5.42 seconds
'''