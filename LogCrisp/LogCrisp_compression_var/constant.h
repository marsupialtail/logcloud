#ifndef CONSTANT_H
#define CONSTANT_H

#include <iostream>
#include <cstdlib>
#include <string>
#include <map>
using namespace std;
//the name struct of a file
//Old: E3_V4~1.svar
//New: (3<<16) + (4<<8) + (1<<4) + 1

#define POS_TEMPLATE 16
#define POS_VAR 8
#define POS_SUBVAR 4
#define POS_TYPE 0

#define MASK_VAR 0xff
#define MASK_SUBVAR 0xf
#define MASK_TYPE 0xf
#define MASK_VARNAME 0xffffff00

#define MAX_SEGLEN 8192 //The max number of segments
#define MAX_TEMPLATE 4096  //The max number of templates
#define MAX_VARIABLE 255 //Max variable per template
#define MAX_SUBVARIABLE 16 //Max subvariable per variable

#define VAR_TYPE_DIC       0  //.dic
#define VAR_TYPE_SUB       1  //.svar
#define VAR_TYPE_IDX       2  //.idx
#define VAR_TYPE_ENTRY     3  //.entry
#define VAR_TYPE_META      4  //.meta
#define VAR_TYPE_TMPLS     5  //.templates
#define VAR_TYPE_VARLIST   6  //.variables
#define VAR_TYPE_OUTLIER   7  //.outlier
#define VAR_TYPE_EID       5  //.eid


#define MAXLOG 100000 //The max number of log entry
#define MAX_VALUE_LEN  1024000 //Max single value length
#define MAX_LENGTH 3000000 //Max length of signle log line9


#define MAXCROSS 10000000
#define MAXTEMP 100000
#define MAXLINE 10000000

//Match fail code number
#define FAIL_SEG_LEN -1
#define FAIL_STATIC_DELIM_LEN -2
#define FAIL_STATIC_DELIM -3
#define FAIL_STATIC_CONST -4
#define FAIL_RUNTIME_CONST_LEN -5
#define FAIL_RUNTIME_CONST -6
#define FAIL_RUNTIME_FIX_LEN -7
#define FAIL_RUNTIME_FIX -8
#define FAIL_RUNTIME_VAR_LEN -9
#define FAIL_RUNTIME_VAR -10
#define FAIL_RUNTIME_LEN -11
#define FAIL_SUBVAR_NOGROUP -12
#define FAIL_SUBVAR_OVERFLOW -13
#define FAIL_DICVAR_NOFOUND -14

#define TOKEN " \t:=,"

#define SysDebug //printf
#define SysWarning printf

using namespace std;
typedef struct VarArray
{
    int tag;
    int * startPos;
    int * len;
    int totsize;
    int nowPos;
    VarArray(int _tag, int initSize)
    {
        tag = _tag;
        startPos = new int[initSize];
        len = new int[initSize];
        totsize = initSize;
        nowPos = 0;
    }
    ~VarArray(){
        free(startPos);
        free(len);
    }
    void Add(int startpos, int length)
    {
        if(nowPos == totsize){ //Full
            //cout << "Copy and expending" << endl;
            int* nstartPos = new int[totsize * 2];
            int* nlen = new int[totsize * 2];
            for(int i = 0; i < totsize; i++){
                nstartPos[i] = startPos[i];
                nlen[i] = len[i];
            }       
            free(startPos);
            free(len);
            startPos = nstartPos;
            len = nlen;    
            totsize = totsize * 2;
        }
        startPos[nowPos] = startpos;
        len[nowPos] = length;
        nowPos++;
    }
}VarArray;


//记录被截取的段的属性
typedef struct SegTag
{
    int startPos;//被截取的段的开始位置
    int segLen;//段的长度
    int tag;//被截取段的属性
}SegTag;

typedef unsigned char Byte;

#endif
