#ifndef RUNPATTERN_H
#define RUNPATTERN_H
#include "constant.h"
#include "Coffer.h"
#include <set>

#define SUBPATTERN 0
#define DICPATTERN 1
#define MULPATTERN 2

#define CONSTFIELD 1
#define FIXFIELD 2
#define VARFIELD 3

#define MATCHONSUB 999
#define MATCHONMULP 998
#define MATCHONMULD 997
#define MATCHONDIC 996

class RuntimePattern{
    
    
    unsigned int* sketches; //pattern sketches    


    //(For dict)
    unsigned int* varTypes; //type number for each variable
    int totVar; //tot size of types
    

    //(For subp)
    
    int * fieldMode; //(For each sketch each field) field matching mode, const, fixed, variable
    unsigned int * fieldType; //(For each sketch each field) field matching type, const field record sketches, other field record type
    unsigned int * subvarType; //(For each sketch each field)
    int* fieldLen; //(For each sketch each field) field matching length, -1 for variable(to check next not alpha, not number )
    
    int cmpConst(int idx, int offset, int len, char* mbuf, int nowStart);
    int cmpFix(int idx, int offset, int len, char* mbuf, int nowStart);
    int cmpVar(int idx, int offset, int len, char* mbuf, int nowStart);

    //temp 
    unsigned int n_varTypes[1024];
public:
    int varName;
    int type; //SUBPATTERN, DICPATTERN

    int sketchNum; //(For dict) match sketch
    int * sketchSize;
    int* varCnt; //variable number for each sketch
    int** tvarLen;
    int totSize;
    bool mixPattern;
    
    int capsuleNum;
    int fieldNum; //(For each sketch) match field
    int subvarCnt; //(For each sketch) sub variable count

    RuntimePattern(int varName, char type, const char* content);
    ~RuntimePattern();
    
    int match(int varName, char* mbuf, int offset, int len, int* l_mem, int & nGroup, int & l_pointer);
   //int extract(const char* keyword, int keywordLen, CofferStore&); //Match + Extract
    string output(); //For debug
    int getDictNum();
    unsigned int getFieldType(int idx);
    int getFieldLen(int idx);
};

#endif