#include "Group.h"
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include "util.h"
using namespace std;
int Group::getFieldNum(char* mbuf, int p, int l){
    char* start = mbuf + p;
    int fnum = 0;
    for(int i = 0; i < l; i++){
        if(!isalnum(start[i])){
            fieldSpliter[fnum] = start[i];
            fnum++;
        }
    }
    return fnum;
}
void Group::insertField(char* mbuf, int p, int l){
    char* start = mbuf + p;
    int fnum = 0;
    int nowFieldPos = p;
    int nowFieldLen = 0;
    for(int i = 0; i < l; i++){
        if(!isalnum(start[i])){
            fieldPos[fnum][nowSize] = nowFieldPos;
            fieldLen[fnum][nowSize] = nowFieldLen;
            if(fieldMinLen[fnum] == -1 || nowFieldLen < fieldMinLen[fnum]){
                fieldMinLen[fnum] = nowFieldLen;
            }
            if(nowFieldLen > fieldMaxLen[fnum]){
                fieldMaxLen[fnum] = nowFieldLen;
            }
            if(fixedLen[fnum] && (nowFieldLen != fieldLen[fnum][0]))
                fixedLen[fnum] = false;
            fieldType[fnum] |= getType(mbuf + nowFieldPos, nowFieldLen);
            fnum++;
            nowFieldPos += nowFieldLen + 1;
            nowFieldLen = 0;
        }else{
            nowFieldLen++;
        }
    }
    
    fieldPos[fnum][nowSize] = nowFieldPos;
    fieldLen[fnum][nowSize] = nowFieldLen;
    if(fieldMinLen[fnum] == -1 || nowFieldLen < fieldMinLen[fnum]){
        fieldMinLen[fnum] = nowFieldLen;
    }
    if(nowFieldLen > fieldMaxLen[fnum]){
        fieldMaxLen[fnum] = nowFieldLen;
    }
    if(fixedLen[fnum] && (nowFieldLen != fieldLen[fnum][0]))
        fixedLen[fnum] = false;
    fieldType[fnum] |= getType(mbuf + nowFieldPos, nowFieldLen);
    
    nowSize++;
}

string Group::getSPattern(char* mbuf, int& varCnt){
    string pattern = "";
    
    for(int i = 0; i < fieldNum + 1; i++){
        int varLen = 0;
        int varType = 0;
        for(int j = 0; j < fieldMinLen[i]; j++){
            char pivot = *(mbuf + fieldPos[i][0] + j);
            int commonNum = 0;
            int nowType = getTypeC(pivot);
            for(int t = 1; t < nowSize; t++){
                if(*(mbuf + fieldPos[i][t] + j) == pivot) commonNum++;
                nowType |= getTypeC(*(mbuf + fieldPos[i][t] + j));
            }
            if(commonNum >= nowSize * 0.95){
                if(varLen != 0){
                    varCnt++;
                    pattern += "<F," + to_string(varType) + "," + to_string(varLen) + ">" + string(1, pivot);
                }else{
                    pattern += string(1, pivot);
                }
                varLen = 0;
                varType = 0;
            }else{
                varLen++;
                varType |= nowType;
            }
        }
        if(varLen != 0){
            if(fixedLen[i]){
                varCnt++;
                pattern += "<F," + to_string(varType) + "," + to_string(varLen) + ">";
            }else{ //<V>/
                int tailvarType = 0;
                for(int t = 0; t < nowSize; t++){ 
                    for(int j = fieldMinLen[i]; j < fieldLen[i][t]; j++)
                        tailvarType |= getTypeC(*(mbuf + fieldPos[i][t] + j));
                }
                varCnt++;
                pattern += "<V," + to_string(tailvarType | varType) + "," + to_string(fieldMaxLen[i]) + ">";
            }
        }else{
            if(!fixedLen[i]){
                int tailvarType = 0;
                for(int t = 0; t < nowSize; t++){ //aweg<V>/
                    for(int j = fieldMinLen[i]; j < fieldLen[i][t]; j++)
                        tailvarType |= getTypeC(*(mbuf + fieldPos[i][t] + j));
                }
                varCnt++;
                pattern += "<V," + to_string(tailvarType) + "," + to_string(fieldMaxLen[i] - fieldMinLen[i]) + ">";
            }
        }
        if(fieldSpliter[i] != 0) pattern += string(1, fieldSpliter[i]);
    }
    return pattern;
}

string Group::getDPattern(char* mbuf){
    string pattern = "";
    for(int i = 0; i < fieldNum + 1; i++){
        int varLen = 0;
        int varType = 0;
        for(int t = 0; t < nowSize; t++){
           varType |= getType(mbuf + fieldPos[i][t], fieldLen[i][t]);
        }
        if(varType != 0) pattern += "<V," + to_string(varType) + ">";
        if(fieldSpliter[i] != 0) pattern += string(1, fieldSpliter[i]);
    }
    return pattern;
}

// string Group::getDPattern(char* mbuf){
//     string pattern = "";
//     for(int i = 0; i < fieldNum + 1; i++){
//         int varLen = 0;
//         int varType = 0;
//         for(int j = 0; j < fieldMinLen[i]; j++){
//             char pivot = *(mbuf + fieldPos[i][0] + j);
//             int commonNum = 0;
//             int nowType = getTypeC(pivot);
//             for(int t = 1; t < nowSize; t++){
//                 if(*(mbuf + fieldPos[i][t] + j) == pivot) commonNum++;  
//                 nowType |= getTypeC(*(mbuf + fieldPos[i][t] + j));
//             }
//             if(commonNum >= nowSize * 0.95){
//                 if(varLen != 0){
//                     pattern += "<V," + to_string(varType) + ">" + string(1, pivot);
//                 }else{
//                     pattern += string(1, pivot);
//                 }
//                 varLen = 0;
//             }else{
//                 varLen++;
//                 varType |= nowType;
//             }
//         }
//         if(!fixedLen[i]) {
//             int tailvarType = 0;
//             for(int t = 0; t < nowSize; t++){ //aweg<V>/
//                 for(int j = fieldMinLen[i]; j < fieldLen[i][t]; j++)
//                     tailvarType |= getTypeC(*(mbuf + fieldPos[i][t] + j));
//             }
//             pattern += "<V," + to_string(tailvarType) + ">";
//         }else if(varLen != 0){
//             pattern += "<V," + to_string(varType) + ">";
//         }
//         if(fieldSpliter[i] != 0) pattern += string(1, fieldSpliter[i]);
//     }

//     return pattern;
// }
