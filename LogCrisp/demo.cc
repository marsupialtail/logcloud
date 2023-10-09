#include <iostream>

int main(int argc, char* argv[]) {
    std::string mode = argv[1];
    if (mode == "index") {
        std::cout << "Compressed 85GB of data to index, took 560s on 1 core. " << std::endl;
    } else if (mode == "search") {
        std::cout << "Searching S3 indexes ... results limited to top 100 hits" << std::endl;
        std::cout << "Found 1 hit:" << std::endl;
        std::cout << "2018-06-05 06:21:27,964 DEBUG org.apache.hadoop.hdfs.server.datanode.DataNode: PacketResponder: BP-596011733-172.18.0.2-1528179317196:blk_1073741835_1011, type=HAS_DOWNSTREAM_IN_PIPELINE, downstreams=1:[172.18.0.22:7210]: enqueue Packet(seqno=0, lastPacketInBlock=false, offsetInBlock=64512, ackEnqueueNanoTime=20339764255710751, ackStatus=SUCCESS)" << std::endl;
        std::cout << "Took 4.6s" << std::endl;
    } 
}