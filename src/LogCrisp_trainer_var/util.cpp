#include"util.h"
#include<cstdlib>
#include<cstdio>
#include<cstring>
#include<ctype.h>
#include<iostream>
#include<cctype>

using namespace std;

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
int Myreadline(FILE* in, int& readSize, int& nowSize, long totSize, char *buffer){
    readSize = 0;
    int badChar = 0;
    while(nowSize++ < totSize){
        char temp;
        fread(&temp, 1, 1, in);
        //cout << int(temp) << endl;
        if (int(temp) == 10){
            break;
        }
		if (int(temp) == 0){
			badChar = 1;
		}
		buffer[readSize++] = temp;        
    }
    //cout << buffer << endl;
    return badChar;
}

int Mystrtok(char *s, const char *delim, char* &buf){

    const char *spanp;
    int c, sc;
    char *tok;
    static char *last;
    int dcount = 0;
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

int getTypeC(char c){
    if(c >= '0' && c <= '9') return NUM_TY;
    if(c >= 'A' && c <= 'F') return AF_TY;
    if(c >= 'a' && c <= 'f') return af_TY;
    if(c >= 'G' && c <= 'Z') return GZ_TY;
    if(c >= 'g' && c <= 'z') return gz_TY;
    return symbol_TY;
}

// int isNumC(char c){
//     if(c >= '0' && c <= '9') return 1;
//     return 2;
// }

int getType(const char* temp, int len){
    int Type = 0;
    for(int i = 0; i < len; i++){
        Type |= getTypeC(temp[i]);
    }
    return Type;
}

unsigned int _stringHash_(const char* start, int len){
    unsigned int seed = 131;
    unsigned int hash = 0;
    for(int i = 0; i < len; i++){
        hash = hash * seed + start[i];
    }
    return (hash & 0x7FFFFFFF);
}

unsigned int _sketchHash_(const char* start, int len){
    unsigned int seed = 233;
    unsigned int hash = 0;
   //int nowType = 0;
    for(int i = 0; i < len; i++){
        if(!isalnum(start[i])){
            // if(nowType != 0){
            //     hash = hash * seed + nowType;
            //     nowType = 0;
            // }
            hash = hash * seed + start[i];
        }else{
     //       nowType |= getTypeC(start[i]);
        }
    }
    // if(nowType != 0){
    //     hash = hash * seed + nowType;
    // }
    return (hash & 0x7FFFFFFF);
}

string _getString_(const char* start, int len){
    char* carray = new char[len + 5];
    memcpy(carray, start, len);
    carray[len] = '\0';
    return string(carray);
}

bool leftCmp (pair<unsigned int, int> t1, pair<unsigned int, int> t2){
    return (t1.second > t2.second);
}

bool rightCmp (pair<unsigned int, int> t1, pair<unsigned int, int> t2){
    return (t1.second < t2.second);
}