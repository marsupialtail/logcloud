#ifndef UTIL
#define UTIL
#define NUM_TY 1
#define AF_TY 2
#define af_TY 4
#define GZ_TY 8
#define gz_TY 16
#define symbol_TY 32
#include<cstdlib>
#include<cstdio>
#include<ctype.h>
#include<iostream>
using namespace std;

const int CharTable[128] = {32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,1,1,1,1,1,1,1,1,1,1,32,32,32,32,32,32,32,2,2,2,2,2,2,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,32,32,32,32,32,32,4,4,4,4,4,4,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,32,32,32,32,32};


// inline FILE* initFile(string input_path, long& lSize);
//int Myreadline(FILE* in, int& readSize, int& nowSize, long totSize, char *buffer);
// inline int Mystrtok(char *s, const char *delim, char* &buf);
// inline int Myatoi(char *nptr, int max_length, int &readLength);
// inline int getTypeC(int c);
// inline int getType(const char* temp, int len);
// inline int Mystrtok(char *s, const char *delim, char* &buf);
// inline bool leftCmp(pair<unsigned int, int> t1, pair<unsigned int, int> t2);
// inline bool rightCmp(pair<unsigned int, int> t1, pair<unsigned int, int> t2);
unsigned int getStrSketch(const char* start, int len, int* varLens, int& varCnt);
unsigned int getMPatSketch(const char* pattern, int len);
unsigned int getPatSketch(const char* pattern, int len, unsigned int* varTypes, int& varCnt, int& totVar);
unsigned int getValSketch(const char* keyword, int keywordLen, unsigned int* varTypes, int * varLens, int& varCnt);

inline FILE* initFile(string input_path, long& lSize){
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

inline int Mystrtok(char *s, const char *delim, char* &buf){

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

inline int Myatoi(char *nptr, int max_length, int &readLength){
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

inline int getTypeC(int c){
    if(c < 0 || c >= 128) return symbol_TY;
    return CharTable[c];
}

inline int getType(const char* temp, int len){
    int Type = 0;
    for(int i = 0; i < len; i++){
        Type |= getTypeC(temp[i]);
    }
    return Type;
}


inline bool leftCmp (pair<unsigned int, int> t1, pair<unsigned int, int> t2){
    return (t1.second > t2.second);
}

inline bool rightCmp (pair<unsigned int, int> t1, pair<unsigned int, int> t2){
    return (t1.second < t2.second);
}

#endif
