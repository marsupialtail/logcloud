#ifndef STATICPATTERN_H
#define STATICPATTERN_H

#include <iostream>
#include <string>
#include <set>
#include <string>
#include <vector>
#include "constant.h"
#include "RuntimePattern.h"
#include "Coffer.h"

#define CONSEG 1
#define VARSEG 2



using namespace std;

class StaticPattern{
public:
    int Eid;
    int segCnt;
    int* dliPos; //Delim Position (First check delimer)
    int* cnstPos; //Const Position (Then check constant)
    int* varPos; //Var Position (Then check variable)
    unsigned int* segContent; //For const, record sketch. For var, record varNum.
    int dliCnt;
    int cnstCnt;
    int varCnt;
    int* RetArray;
    int* MatchType;
public:
    StaticPattern(int Eid, const char* content, int pLen, const char* delim, int& length);
    ~StaticPattern();
    int matchOnDelim(char * mbuf, int * offset, int * len);
    int matchOnConstant(char* mbuf, int* offset, int * len);
    int matchOnVariable(char* mbuf, int *offset, int* len, int* st_start, int* l_mem, RuntimePattern** rp_store, int & l_pointer);
    
    void updatePattern(char* mbuf, int& now_pointer, int * g_mem, int * l_mem, int* g_entry, int& e_pointer, int* st_start, RuntimePattern** rp_store, int * rp_start, Coffer** cp_store);
    void updateMDPattern(char* mbuf, int& now_pointer, int& l_pointer, int nGroup, int * g_mem, int * l_mem, int* st_start, RuntimePattern** rp_store, int* rp_start, Coffer** cp_store);
    void updateMDPatternDic(char* mbuf, int& now_pointer, int nGroup, int n_offset, int n_len, int sketchNum, int* d_varLen, int* g_mem, int dic_pos, int dic_varName, Coffer** cp_store);
    void updateMDPatternIdx(int& now_pointer, int ret, int* g_mem, RuntimePattern** rp_store, int * rp_start, int var_pos, int subvarCnt, int idx_varName, Coffer** cp_store);
    void updateSPattern(int& now_pointer, int& l_pointer, int ret, int typ, int* g_mem, int* l_mem, int* st_start, RuntimePattern** rp_store, int * rp_start, Coffer** cp_store);
    
    void updateDPattern(char* mbuf, int& now_pointer, int& l_pointer, int nGroup, int * g_mem, int * l_mem, int* g_entry, int& e_pointer, int* st_start, RuntimePattern** rp_store, int* rp_start, Coffer** cp_store);
    void updateDPatternDic(int& now_pointer, int nGroup, int offset, int len, int * g_mem,  int dic_varName, int dic_pos, int sketchNum, int * d_varLen, int d_varCnt, Coffer** cp_store);
    bool updateDPatternEntry(int ret, int* g_entry, int& e_pointer, int ent_varName, int ent_pos, int dicCnt, unsigned int strSketch, Coffer** cp_store);

    int match(char* mbuf, int * offset, int* len, int segSize, int* g_mem, int* l_mem, int * g_entry, int & n_pointer, int & e_pointer, RuntimePattern** rp_store, Coffer** cf_store, int* st_start, int* rp_start, int nowLine);
    string output();
};   

#endif
