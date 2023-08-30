#include"util.h"
#include<cstdlib>
#include<cstdio>
#include<ctype.h>
#include<iostream>
#include<cctype>

using namespace std;
const int CharTable[128] = {32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,1,1,1,1,1,1,1,1,1,1,32,32,32,32,32,32,32,2,2,2,2,2,2,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,32,32,32,32,32,32,4,4,4,4,4,4,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,32,32,32,32,32};
FILE* initFile(string input_path, long& lSize){
    FILE* fo;
    fo = fopen(input_path.c_str(), "r");
    if (fo == NULL){
        cout << "Open intput log file failed" << endl;
        return NULL;
    }
    fseek(fo, 0, SEEK_END);
    lSize = ftell(fo);
    rewind(fo);
    return fo;
}
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

int Mystrtok(char *s, const char *delim, char* &buf){

    const char *spanp;
    int c, sc;
    char *tok;
    static char *last;
    buf = NULL;
    if (s == NULL && (s = last) == NULL)
        return 0;
    
    c = *s++;
    for (spanp = delim; (sc = *spanp++) != 0;) {
        if (c == sc){           
            last = s;
            return c;
        }
    }
    
    if (c == 0) {                 
        last = NULL;
        return c;
    }
    
    tok = s - 1;
    for(;;){
        c = *s++;
        spanp = delim;
        do {
            if ((sc = *spanp++) == c) {
                int return_value = 0;
                if (c == 0){
                    s = NULL;
                    return_value = 0;
                }
                else{
                    return_value = s[-1];
                    s[-1] = 0;
                }   
                last = s;
                (buf) = (tok);
                return return_value;
            }
        } while (sc != 0);
    }
//    return strtok(s, delim);
}

int Myatoi(char *nptr, int max_length, int &readLength){
    int c; int total;
 	total = 0;
    c = (int)(unsigned char)*nptr++;
    while (isdigit(c)) {
        readLength++;
        if (readLength > max_length){
            readLength = -1;
            break;
        } 
        total = total * 10 + (c - '0');
        c = (int)(unsigned char)*nptr++;
    }
	return total;
}

int getTypeC(int c){
    if(c < 0 || c >= 128) return symbol_TY;
    return CharTable[c];
}

int getType(const char* temp, int len){
    int Type = 0;
    for(int i = 0; i < len; i++){
        Type |= getTypeC(temp[i]);
    }
    return Type;
}


bool leftCmp (pair<unsigned int, int> t1, pair<unsigned int, int> t2){
    return (t1.second > t2.second);
}

bool rightCmp (pair<unsigned int, int> t1, pair<unsigned int, int> t2){
    return (t1.second < t2.second);
}

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
