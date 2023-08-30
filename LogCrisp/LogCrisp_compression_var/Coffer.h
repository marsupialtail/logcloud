#ifndef COFFER_H
#define COFFER_H
#include<iostream>
#include<cstdlib>
#include<cstdio>
#include<vector>
#include<set>
#include<map>
#include"constant.h"
//the name struct of a file
//Old: E3_V4~1.svar
//New: (3<<16) + (4<<8) + (1<<4) + 1
#define VAR_TYPE_DIC       0  //.dic
#define VAR_TYPE_SUB       1  //.svar
#define VAR_TYPE_VAR       2  //.var
#define VAR_TYPE_ENTRY     3  //.entry
#define VAR_TYPE_META      4  //.meta
#define VAR_TYPE_TMPLS     5  //.templates
#define VAR_TYPE_VARLIST   6  //.variables
#define VAR_TYPE_OUTLIER   7  //.outlier

//Meta
#define META_TAG_CMP 0 //0 for uncompressed, 1 for compressed
#define META_TAG_DST 1 //0 for 1 byte dst, 1 for 4 byte dst
#define META_TAG_SRC 2 //0 for 1 byte src, 1 for 4 byte src
#define META_TAG_LIN 3 //0 for 1 byte lin, 1 for 4 byte lin
#define META_TAG_OUTLIER 4 //1 for outlier coffer, 0 for others
#define META_TAG_OPT 5 //1 for idx coffer optimize, 0 for others
#define META_TAG_ISINT 6 //1 for integer coffer, 0 for others

#define MAX_META_SEG 16 //change each integer into 2 bytes (16 segments)

using namespace std;
const int BASE10[10] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
const string typeNames[8] = {"Dic", "Svar", "Var", "Entry", "Meta", "Templates", "Variables", "Outlier"};
class Coffer{
    public:
        Byte* data;
        Byte* cdata;
        
    //For seg read
        int varName;

        map<unsigned int, int> dataDic;
        int startIdx;
        int endIdx;
        int baseOffset;

        int startIdx_e;
        int endIdx_e;
        //int* entry = NULL; //entry array 

        //Entry
        int dicCnt; //dic # corresponds to entry (as large as shown in pattern file)
        int* dicSize;
        int* groupStart;
        bool optimize;
        
        //Int
        bool isInt;
        Byte * metaTag;
        int * metaEnd;
        int * metaMax;
        int * metaMin;
        Byte nowSeg;

        int intMin;
        int intMax;

        //Dict
        int dicStart;
        int dVarCnt;
        int * dVarLen;

        //Mix pattern
        int outlierCnt;
        int * outlierIdx;

        //Sub
        unsigned int fieldType;
        int fieldLen;
        bool isFix;
        bool mixPattern;

        //general
        int srcLen; //original total size
        int destLen; //compressed total size
        int eleLen; //size of each elements
        int org_eleLen; //len of integer length
        int type; 
        int lines; //# of lines


        Byte nprop[5]{'\0'};
        
        int compressed;
        int offset;

        Coffer();
        Coffer(int varName);
        Coffer(int varName, int size);

        //~Coffer();//clove++
        
        static void readCoffer(const Byte* buffer, int len, map<int, Coffer*>& t_map); //From meta to coffer
        static string metaRead(const Byte* buffer, int len);
//        int append(string filename, char* srcData, int len);
        int checkInDic(unsigned int datasketch);
        void addInDic(unsigned int datasketch, int value);
        int getDicSize();

        void addData(const char* start, int len);
        void clear();
        bool addDicData(char* mbuf, int offset, int len);
        void dataSet(string& data);

        int readFile(FILE* zipFile, int fstart, string compression_method); //Read to cdata

        int copy(char* mbuf, int* g_mem, int * g_entry, int totSize);
        void copyEntry(char* mbuf, int* g_entry, int totSize);
        void copyInteger(char* mbuf, int* g_mem, int totSize);
        void copyVar(char* mbuf, int* g_mem, int totSize);
        void dicInit();
        int getIdx(int value);

        int compress(int compression_level); //compress data to cdata
        int decompress(string compression_method); //decompress cdata to data

        void output(FILE* zipFile); //output compressed cdata
        void printFile(string rootPath); //output to root Path

        string print(char* mbuf, int * g_offset, int * g_len, int * g_pointer, int * g_entry, int * g_entry_p);
        string print();
        string printMeta();

};
#endif

