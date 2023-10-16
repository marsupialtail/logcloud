#include"util.h"
#include<cstdlib>
#include<cstdio>
#include<ctype.h>
#include<iostream>
#include<cctype>

using namespace std;

// int Myreadline(FILE* in, int& readSize, int& nowSize, long totSize, char *buffer){
//     readSize = 0;
//     int badChar = 0;
//     while(nowSize++ < totSize){
//         char temp;
//         fread(&temp, 1, 1, in);
//         //cout << int(temp) << endl;
//         if (int(temp) == 10){
//             break;
//         }
// 		if (int(temp) == 0){
// 			badChar = 1;
// 		}
// 		buffer[readSize++] = temp;        
//     }
//     //cout << buffer << endl;
//     return badChar;
// }



unsigned int getValSketch(const char* pattern , int len, unsigned int* varTypes, int* varLens, int& varCnt){
    unsigned int seed = 233;
    unsigned int hash = 0;
    int nowType = 0;
    int nowLen = 0;
    for(int i = 0; i < len; i++){
        if((pattern[i] < 0 || pattern[i] >= 128) || (CharTable[(int)pattern[i]] == symbol_TY)){
            if(nowLen != 0){
                if(nowType != 0 && varTypes != NULL) varTypes[varCnt] = nowType;
                if(varLens != NULL) varLens[varCnt] = nowLen;
                varCnt++;
            }
            hash = hash * seed + int(pattern[i]);
            nowType = 0;
            nowLen = 0;
        }else{
            nowType |= getTypeC(pattern[i]);
            nowLen++;
        }
    }
    if(nowLen != 0){
        if(nowType != 0 && varTypes != NULL) varTypes[varCnt] = nowType;
        if(varLens != NULL) varLens[varCnt] = nowLen;
        varCnt++;
    }
    return hash;
}

unsigned int getMPatSketch(const char* pattern, int len){ 
    //input: <V,1>-<V,2>/<V,3> output: sketches
    unsigned int seed = 233;
    unsigned int hash = 0;
    int lPos = 0, rPos = 0, lastPos = 0;
    for(int i = 0; i < len; i++){
        if(pattern[i] == '<'){
            lPos = i;
        }
        if(pattern[i] == '>'){
            rPos = i;
        }
        if(lPos >= 0 && rPos > lPos){
            if(lPos != 0 && lPos > lastPos){
                //printf("pattern read: ");
                for(int j = lastPos; j < lPos; j++){
                    if((pattern[j] < 0 || pattern[j] >= 128) || (CharTable[(int)pattern[j]] == symbol_TY)){
                        //printf("%d ", (int)pattern[j]);
                        hash = hash * seed + int(pattern[j]);
                    }
                }
                //printf("\n");
            }
            char m_mark;
            int t_mark, l_mark;
            int scanRst = sscanf(pattern + lPos, "<%c,%d,%d>", &m_mark, &t_mark, &l_mark);
            if(scanRst == 3){ //read a variable
                lastPos = rPos + 1;
            }
            lPos = -1;
        }
    }
    if(lastPos < len){
        for(int j = lastPos; j < len; j++){
            if((pattern[j] < 0 || pattern[j] >= 128) || (CharTable[(int)pattern[j]] == symbol_TY))
                hash = hash * seed + int(pattern[j]);
        }
    }
    return hash;
}

unsigned int getPatSketch(const char* pattern, int len, unsigned int* varTypes, int& varCnt, int& totVar){ 
    //input: <V,1>-<V,2>/<V,3> output: sketches
    unsigned int seed = 233;
    unsigned int hash = 0;
    int lPos = 0, rPos = 0, lastPos = 0;
    for(int i = 0; i < len; i++){
        if(pattern[i] == '<'){
            lPos = i;
        }
        if(pattern[i] == '>'){
            rPos = i;
        }
        if(lPos >= 0 && rPos > lPos){
            if(lPos != 0 && lPos > lastPos){
                //printf("pattern read: ");
                for(int j = lastPos; j < lPos; j++){
                    if((pattern[j] < 0 || pattern[j] >= 128) || (CharTable[(int)pattern[j]] == symbol_TY)){
                  //      printf("%d ", (int)pattern[j]);
                        hash = hash * seed + int(pattern[j]);
                    }
                }
                //printf("\n");
            }
            int nowType_mark;
            int scanRst = sscanf(pattern + lPos, "<V,%d>", &nowType_mark);
            if(scanRst == 1){ //read a variable
                if(varTypes) varTypes[totVar++] = nowType_mark;
                varCnt++;
                lastPos = rPos + 1;
            }
            lPos = -1;
        }
    }
    if(lastPos < len){
        for(int j = lastPos; j < len; j++){
            if((pattern[j] < 0 || pattern[j] >= 128) || (CharTable[(int)pattern[j]] == symbol_TY))
                hash = hash * seed + int(pattern[j]);
        }
    }
    return hash;
}

unsigned int getStrSketch(const char* pattern, int len, int* varLens, int& varCnt){
    unsigned int seed = 233;
    unsigned int hash = 0;
    int nLen = 0;
    for(int i = 0; i < len; i++){
        if((pattern[i] < 0 || pattern[i] >= 128) || (CharTable[(int)pattern[i]] == symbol_TY)){
            if(nLen != 0){
                if(varLens != NULL) varLens[varCnt] = nLen;
                varCnt++;
                nLen = 0;
            }
        }else{
            nLen++;
        }
        hash = hash * seed + pattern[i];
    }
    if(nLen > 0){
        if(varLens != NULL) varLens[varCnt] = nLen;
        varCnt++;
        nLen = 0;
    }return hash;
}
