
#include<iostream>
#include<cstdlib>
#include<cstring>
#include<cstdio>
using namespace std;

unsigned int getPatSketch(const char* pattern, int len, unsigned int* varTypes, int& varCnt, int& totVar){ 
    //input: <V,1>-<V,2>/<V,3> output: sketches
    unsigned int seed = 233;
    unsigned int hash = 0;
    int lPos, rPos, lastPos = 0;
    for(int i = 0; i < len; i++){
        if(pattern[i] == '<'){
            lPos = i;
        }
        if(pattern[i] == '>'){
            rPos = i;
        }
        if(lPos >= 0 && rPos > lPos){
            if(lPos != 0 && lPos > lastPos){
                for(int j = lastPos; j < lPos; j++){
                    hash = hash * seed + int(pattern[j]);
                }
            }
            int nowType_mark;
            int scanRst = sscanf(pattern + lPos, "<V,%d>", &nowType_mark);
            if(scanRst == 1){ //read a variable
                varTypes[totVar++] = nowType_mark;
                varCnt++;
                lastPos = rPos + 1;
            }
            lPos = -1;
        }
    }
    if(lastPos < len){
        for(int j = lastPos; j < len; j++){
            hash = hash * seed + int(pattern[j]);
        }
    }
    return hash;
}

int main(){
    string c = "<V,1>-<V,1>-<V,1>";
    unsigned int * a = new unsigned int[100];
    int m = 0;
    int t = 0;

    unsigned int ss = getPatSketch(c.c_str(), c.size(), a, m, t);
    cout << ss<< endl;
    return 0;
}