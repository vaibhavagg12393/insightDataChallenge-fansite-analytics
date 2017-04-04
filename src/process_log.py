// your Python code to implement the features could be placed here
// note that you may use any language, there is no preference towards Python
import time
import os
import re
import operator

fname = os.path.realpath("/Users/Vaibhav/Documents/fansite-analytics-challenge/log_input/log_input.txt")
feature1_out = os.path.realpath("/Users/Vaibhav/Documents/fansite-analytics-challenge/log_output/hosts.txt")
feature2_out = os.path.realpath("/Users/Vaibhav/Documents/fansite-analytics-challenge/log_output/resources.txt")
feature3_out = os.path.realpath("/Users/Vaibhav/Documents/fansite-analytics-challenge/log_output/hours.txt")


# Create a graph to store sender with their receivers and vice-versa
# used a Set because Sets are significantly faster when it comes to determining if an object is
# present in the set (as in x in s)

def past_graph():
    open(feature3_out,'w')
    with open(fname) as fp:
        for line in fp:
            try:
                components = line.strip().split(" ")
                host = re.search(r'([\w.-]+[.]*[\w.-])', line)
                resource = re.search(r'\"(.+?)\"', line)
                http = components[-2].strip()
                bytes = components[-1].strip()
                if host and http=='200':
                    mHost = str(host.group())
                    graph.setdefault(mHost, 0)
                    graph[mHost] += 1
                if resource:
                    try:
                        mResource = str(resource.group()).split()[1].strip()
                        if mResource != "/" and isinstance(int(bytes), int):
                            graph2.setdefault(mResource, 0)
                            graph2[mResource] += int(bytes)
                    except:
                        continue
            except ValueError:
                    continue

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


def getTime(timeinput):
    timeinput = timeinput.split()[:-1]
    timestamp = time.mktime(time.strptime(timeinput[0], '%d/%b/%Y:%H:%M:%S'))
    return timestamp

def slidingWindow(dict_time, time_arr, window):
    i = 0
    while i < len(time_arr):
        val1 = getTime(time_arr[i][0])
        dict_time[time_arr[i][0]] = 1
        j = i + 1
        while j < len(time_arr):
            val2 = getTime(time_arr[j][0])
            if val2 - val1 > window:
                break
            else:
                dict_time[time_arr[i][0]] += 1
            print('.')
            j += 1
        i += 1
    print('Done sliding window')

if __name__ == '__main__':
    graph = {}
    dict_time={}
    time_arr = []
    graph2={}
    start = time.time()
    past_graph()
    end = time.time()
    print "Graph contruction: %.2f seconds"%(end-start)
    start = time.time()
    feature(graph,feature1_out)
    end = time.time()
    print "Feature 1 time: %.2f seconds"%(end-start)
    start = time.time()
    feature(graph2,feature2_out)
    end = time.time()
    print "Feature 2 time: %.2f seconds" % (end - start)
    slidingWindow(dict_time, time_arr, 60)
    print len(dict_time)
    feature(dict_time,feature3_out)

'''
Feature 1 without http 200

piweba3y.prodigy.com,22309
piweba4y.prodigy.com,14903
piweba1y.prodigy.com,12876
siltb10.orl.mmc.com,10578
alyssa.prodigy.com,10184
edams.ksc.nasa.gov,9095
piweba2y.prodigy.com,7961
163.206.89.4,6520
www-d3.proxy.aol.com,6299
vagrant.vf.mmc.com,6096


Feature 1 with http 200

piweba3y.prodigy.com,20748
piweba4y.prodigy.com,13888
piweba1y.prodigy.com,11527
siltb10.orl.mmc.com,9936
alyssa.prodigy.com,9484
edams.ksc.nasa.gov,8653
piweba2y.prodigy.com,7248
163.206.89.4,6231
198.133.29.18,5812
www-d3.proxy.aol.com,5675

/shuttle/missions/sts-71/movies/sts-71-launch.mpg,4830851820
/shuttle/missions/sts-71/movies/sts-71-tcdt-crew-walkout.mpg,4505833864
/shuttle/missions/sts-53/movies/sts-53-launch.mpg,3160325280
/shuttle/countdown/count70.gif,1959049585
/shuttle/technology/sts-newsref/stsref-toc.html,1256215694
/shuttle/countdown/video/livevideo2.gif,1169155568
/shuttle/countdown/count.gif,1154002480
/shuttle/missions/sts-71/movies/sts-71-hatch-hand-group.mpg",1135756810
/shuttle/countdown/video/livevideo.gif,1128333928
/shuttle/missions/sts-71/movies/sts-71-mir-dock.mpg,1095681435


'''
