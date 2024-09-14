import sys
import os
import argparse
import time
import datetime
import threading
import logging
from subprocess import call
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED  
from os.path import join, getsize

TYPE = ["Hadoop"]
#TYPE = ["Windows"]
# TYPE = ["Hadoop", "Hive", "OpenStackC", "Spark", "Thunderbird", "HadoopL", "Windows"]
#TYPE = [
    # "Bgl",
    # "Presto",
    # "PSummary",
    # "Slss"
    #"Thunderbird"
    #"Spark"
    #"Hadoop"
    #"Worker"
#    "Hadoop", "Hive", "OpenStackC", "Spark", "Thunderbird", "HadoopL"
    #"Thunderbird"
    #"Spark", "Thunderbird", "OpenStackC", "Hadoop"
#    "Apsara", "Hadoop", "Ols", "Spark", "PSummary", "Thunderbird", "Worker"
    # "Apsara", "Fastcgi", "K8S", "Metering", "Monitor",
    # "Ols", "Pangu", "Presto", "PSummary",
    # "Request", "Rpc", "Server", "Shennong", "Sinput",
    # "Sla", "Slsf", "Slss", "Sys",
    # "Op",
    # "Tubo",
    # "Worker", #21 types

    # "Android", "Apache", "Bgl", "Hadoop", "Hdfs",
    # "Healthapp", "Hpc", "Linux", "Mac", "Openstack",
    # "Proxifier", "Spark", "Ssh", "Thunderbird", "Windows",
    # "Zookeeper" #16 types
#]
# TYPE = [
#     "Worker"
# ]
# TYPE = [
#     "Linux", "Mac"
# ]

lock = threading.RLock()
gl_threadTotTime = 0
gl_errorNum = 0

#func define
def add_argument(parse):
    parse.add_argument("--Input", "-I", help="The input directory include log files(such as LogA/0.log, LogB/1.log, etc...)")
    parse.add_argument("--Output", "-O", help="The output directory inclues output zip(each input file corresponds to a directory(such as LogA/0.zip,1.zip") 
    parse.add_argument("--Template", "-T", help="The template directory includes tempalte file for each log file(such as LogTempalte/Android.tempaltes)")

    parse.add_argument("--MaxThreadNum", "-TN", default="1", help="The max thread running num")
    parse.add_argument("--ProcFilesNum", "-FN", default="0", help="The max block num a single thread can process, 0 for dynamic distrib.")
  #  parse.add_argument("--Mode", "-m", default="Tot", choices=["Tot", "Seg"], help="The mode of compression(Tot for single large file, Seg for multiple blocks default for Tot)")

def path_pro(path):
    if(path[-1] != '/'):
        path += '/'
    return path

def check_args(args):
    print("Input file: {}, Output file: {}, Template file: {}".format(args.Input, args.Output, args.Template))
    if (not os.path.exists(args.Input)):
        print("No input path. Quit")
        return 0

    if (not os.path.exists(args.Output)):
        print("No output path. Will make new directory at {}".format(args.Output))
        #call("rm -rf " + args.Output,shell=True)
        os.mkdir(args.Output)
    return 1
    
def atomic_addTime(step):
    lock.acquire()
    global gl_threadTotTime
    gl_threadTotTime += step
    lock.release()

def atomic_addErrnum(step):
    lock.acquire()
    global gl_errorNum
    gl_errorNum += step
    lock.release()

def writeLog(fname, message, levelStr):
    logging.basicConfig(filename=fname,
                           filemode='a',
                           format = '%(asctime)s - %(message)s')
    logger = logging.getLogger(__name__)
    if (levelStr =='WARNING'):
        logger.warning(message)
    elif (levelStr =='INFO'):
        logger.info(message)   

#return exec time (t2-t1)
def procFiles(filename, file_list, fileBeginNo, fileEndNo, now_input, now_output, now_template):
    t1 = time.time()
    #parser
    input_path = path_pro(now_input)
    output_path = path_pro(now_output)

    if(not os.path.exists(input_path)):
        print(input_path + "does not exists")
        return 

    # order = "./Compressor -I " + input_path + " -O " + output_path + " -T " + now_template + " -Z Z -X " + str(fileBeginNo) + " -Y " + str(fileEndNo + 1)
    # print(order + " " + threading.current_thread().name)
    # res = call(order, shell=True)
    # if(res != 0):
    #     tempStr = "thread: {}, TypeName: {}, fileName: {} ERROR!!".format(threading.current_thread().name, filename, str(now_output))
    #     print(tempStr)
    #     writeLog("./Log_{}".format(datetime.date.today()), tempStr,'WARNING')
    #     atomic_addErrnum(1)

    # t2 = time.time()
    # tempStr = "thread:{}, fileNo: {} to {} , cost time: {}".format(threading.current_thread().name, fileBeginNo, fileEndNo, t2 - t1)
    # print (tempStr)
    # return t2 - t1
    maxTimeCost = -1
    pl = ""
    for i in range(fileBeginNo, fileEndNo + 1):
        target_file = file_list[i]
        now_input = input_path + str(target_file)
        now_output = output_path + str(target_file)
        if(not os.path.exists(now_input)):
            print(now_input + "does not exists")
            continue
#        order = "./Compressor -I " + now_input + " -O " + now_output + " -T " + now_template + " -Z Z -C Zstd"
        order = "\n./Compressor -I " + now_input + " -O " + now_output + " -T " + now_template + " -Z Z"
        print(order + " " + threading.current_thread().name)
        time1 = time.time()
        res = call(order,shell=True)
        timeCost = time.time() - time1
        if(timeCost > maxTimeCost):
            maxTimeCost = timeCost
            pl = now_input
        print("Total Cost: {}".format(timeCost))
        if (res != 0):
            tempStr = "thread: {}, TypeName: {}, fileName: {} ERROR!!".format(threading.current_thread().name, filename, str(now_output))
            print (tempStr)
            writeLog("./Log_{}".format(datetime.date.today()), tempStr,'WARNING')
            atomic_addErrnum(1)
    
    t2 = time.time()
    tempStr = "thread:{}, fileNo: {} to {} , cost time: {}".format(threading.current_thread().name, fileBeginNo, fileEndNo, t2 - t1)
    writeLog("./Log_{}".format(datetime.date.today()), "Max file: " + now_input + " time: " + str(maxTimeCost), 'WARNING')
    print (tempStr)
    #writeLog(str(output_path) + "Log_{}".format(datetime.date.today()), tempStr,'WARNING')

    return t2 - t1

def procFiles_result(future):
    atomic_addTime(future.result())

# calculate the reduce rate of each type file
def getdirsize(dir):
    size = 0
    for root, dirs, files in os.walk(dir):
        size += sum([getsize(join(root, name)) for name in files])
    return size


def threadsToExecTasks(filename, file_list, now_input, now_output, now_template):
    fileListLen = len(file_list)
    curFileNumBegin = 0
    curFileNumEnd = 0
    step = maxSingleThreadProcFilesNum
    if (step == 0):# dynamic step
        step = fileListLen // maxThreadNum
        if(step == 0):
            step = 1 # make sure the step is bigger than 0
    
    threadPool = ThreadPoolExecutor(max_workers = maxThreadNum, thread_name_prefix="LR_")
    while curFileNumBegin < fileListLen:
        if (curFileNumBegin + step > fileListLen):
            curFileNumEnd = fileListLen - 1
        else:
            curFileNumEnd = curFileNumBegin + step - 1

        future = threadPool.submit(procFiles, filename, file_list, curFileNumBegin, curFileNumEnd, now_input, now_output, now_template)
        future.add_done_callback(procFiles_result)
        curFileNumBegin = curFileNumEnd + 1
    #wait(future, return_when=ALL_COMPLETED)
    threadPool.shutdown(wait=True)



if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    add_argument(parse)
    args = parse.parse_args()
    if (not check_args(args)):
       exit(1)

    #init params
    input_path = args.Input
    output_path = args.Output
    template_path = args.Template

    maxThreadNum = int(args.MaxThreadNum)
    maxSingleThreadProcFilesNum = int(args.ProcFilesNum)
    #threadPool = ThreadPoolExecutor(max_workers = maxThreadNum, thread_name_prefix="test_")
    
    time1 = time.time()
    for filename in TYPE:
        print("Now process: " + filename)
        now_input = os.path.join(input_path, filename)
        time_t1 = time.time()
        now_output = os.path.join(output_path,filename)
        all_files = os.listdir(now_input)
        now_template = os.path.join(template_path, filename)
        if (not os.path.exists(now_output)):
            os.mkdir(now_output)
        
        ###ThreadPool to Proc Files
        #file_list = list(recheck(len(all_files), now_input, now_output, is_padding))
        
        threadsToExecTasks(filename, all_files, now_input, now_output, now_template)

        time_t2 = time.time()
        tempStr = "{} finished, total time cost: {} , thread accum time: {}".format(filename, time_t2 - time_t1, gl_threadTotTime)
        print(tempStr)
        writeLog("./Log_{}".format(datetime.date.today()), tempStr,'WARNING')
        gl_threadTotTime = 0 # reset

    time2 = time.time() 
    tempStr = "Main finished, total time cost: {} , error num: {}".format(time2 - time1, gl_errorNum)
    print(tempStr)
    writeLog("./Log_{}".format(datetime.date.today()), tempStr,'WARNING')
