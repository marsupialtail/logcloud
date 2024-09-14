#ifndef GROUP_H
#define GROUP_H
#define SUBPATTERN 0
#define DICPATTERN 1
#include<cstdlib>
#include<iostream>
using namespace std;

class Group{
    unsigned int sketchHashing;
    int fieldNum;
    int** fieldPos;
    int** fieldLen;
    int* fieldMinLen;
    int* fieldMaxLen;
    char* fieldSpliter;
    bool* fixedLen;
    int* fieldType;
    int getFieldNum(char* mbuf, int p, int l);
    int nowSize;
public:
    void insertField(char* mbuf, int p, int l);
    Group(unsigned int _sketch, int maxSize, char* mbuf, int _pos, int _len){
        fieldSpliter = new char[1000]{0};
        fieldNum = getFieldNum(mbuf, _pos, _len);        
        
        fieldPos = new int*[fieldNum + 1];
        fieldLen = new int*[fieldNum + 1];
        
        fieldMinLen = new int[fieldNum + 1];
        fieldMaxLen = new int[fieldNum + 1];
        fixedLen = new bool[fieldNum + 1];
        fieldType = new int[fieldNum + 1];
        
        for(int i = 0; i < fieldNum + 1; i++){
            fieldMinLen[i] = -1;
            fieldMaxLen[i] = -1;
            fixedLen[i] = true;
            fieldType[i] = 0;
        }
        for(int i = 0; i < fieldNum + 1; i++){
            fieldPos[i] = new int[maxSize];
            fieldLen[i] = new int[maxSize];
        }
        nowSize = 0;
        insertField(mbuf, _pos, _len);
    }
    ~Group(){
        delete[] fieldSpliter;
        for(int i = 0; i < fieldNum + 1; i++){
            if(fieldPos[i]) delete[] fieldPos[i];
            if(fieldLen[i]) delete[] fieldLen[i];
        }
        delete[] fieldPos;
        delete[] fieldLen;
        delete[] fieldMinLen;
        delete[] fieldMaxLen;
        delete[] fixedLen;
        delete[] fieldType;
    }
    
    string getSPattern(char* mbuf, int& varCnt);
    string getDPattern(char* mbuf);

};
#endif

