from subprocess import call
TYPE = [
    #"Windows"
    #"Hadoop", "Thunderbird", "Spark", "OpenStackC", "Hive", "HadoopL"
    #"Thunderbird", "Spark", "Hadoop", 
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
]
for t in TYPE:
    command = "./Trainer -I " + "~/LogData/LogSample/" + t + ".sample" + " -O " + "~/LogTemplate/" + t
    print(command)
    ret = call(command,shell=True)
    if(ret != 0):
        print(t + " Error!!")