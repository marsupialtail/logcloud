#include "RuntimePattern.h"
#include "constant.h"
#include "util.h"
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cctype>
#define TESTP 9
using namespace std;
RuntimePattern::RuntimePattern(int _varName, char _type, const char* pattern){
    varName = _varName;
    if(_type == 'S'){
        type = SUBPATTERN;
    }else if(_type == 'M'){
        type = MULPATTERN;
    }else if(_type == 'D'){
        type = DICPATTERN;
    }
    mixPattern = false;
    if(type == SUBPATTERN){
        int lPos = 0, rPos = 0, lastPos = 0;
        int pLen = strlen(pattern);
        fieldNum = 0;
        subvarCnt = 0;
        fieldMode = new int[pLen];
        fieldType = new unsigned int[pLen];
        subvarType = new unsigned int[pLen];
        fieldLen = new int[pLen];

        for(int i = 0; i < pLen; i++){
            if(pattern[i] == '<'){
                lPos = i;
            }
            if(pattern[i] == '>'){
                rPos = i;
            }
            if(lPos >= 0 && rPos > lPos){
                if(lPos != 0 && lPos > lastPos){ //Const field
                    fieldMode[fieldNum] = CONSTFIELD;
                    int tmp;
                    fieldType[fieldNum] = getStrSketch(pattern + lastPos, lPos - lastPos,  NULL, tmp);
                    fieldLen[fieldNum] = lPos - lastPos;
                    fieldNum++;
                }
                char fixlen_mark;
                int strTag_mark;
                int maxlen_mark;
                int scanRst = sscanf(pattern + lPos, "<%c,%d,%d>", &fixlen_mark, &strTag_mark, &maxlen_mark);
                if(scanRst == 3){
                    if(fixlen_mark == 'F'){
                        fieldMode[fieldNum] = FIXFIELD;
                        fieldType[fieldNum] = strTag_mark;
                        subvarType[subvarCnt++] = strTag_mark;
                        fieldLen[fieldNum] = maxlen_mark;
                    }else if(fixlen_mark == 'V'){
                        fieldMode[fieldNum] = VARFIELD;
                        fieldType[fieldNum] = strTag_mark;
                        subvarType[subvarCnt++] = strTag_mark;
                        fieldLen[fieldNum] = maxlen_mark;
                    }
                    fieldNum++;
                    lastPos = rPos + 1;
                }
                lPos = -1;
            }
        }
        if(lastPos < pLen){
            fieldMode[fieldNum] = CONSTFIELD;
            int tmp;
            fieldType[fieldNum] = getStrSketch(pattern + lastPos, pLen - lastPos, NULL, tmp);
            fieldLen[fieldNum] = pLen - lastPos;
            fieldNum++;
        }
        capsuleNum = subvarCnt;
    }
    if(type == MULPATTERN){
        int start = 0, index = 0, nsketch = 0;
        int lPos = 0, rPos = 0, lastPos = 0;

        sketchNum = 0;
        int pLen = strlen(pattern);

        char* buffer = new char[pLen];
        for(index = 0; index < pLen; index++){
            if(pattern[index] == ' '){
                if(sketchNum == 0){
                    memcpy(buffer, pattern + start, index - start);
                    buffer[index - start] = '\0';
                    //printf("start: %d, index: %d, buffer: %s\n", start, index, buffer);
                    sketchNum = atoi(buffer);
                    sketches = new unsigned int[sketchNum];
                    fieldNum = 0;
                    subvarCnt = 0;

                    fieldMode = new int[pLen];
                    fieldType = new unsigned int[pLen];
                    subvarType = new unsigned int[pLen];
                    fieldLen = new int[pLen];
                    
                    varTypes = new unsigned int[sketchNum * MAX_VARIABLE];
                    totVar = 0;
                    varCnt = new int[sketchNum];
                    for(int i = 0; i < sketchNum; i++) varCnt[i] = 0;
                    

                    start = index + 1;
                    lastPos = start;
                    continue;
                }else{
                    //if(varName == 14287360) printf("varName: 14287360, index: %d, start: %d\n", index, start);
                    sketches[nsketch] = getMPatSketch(pattern + start, index - start);
                    //if(varName == 14287360) printf("varName: 14287360 end\n");
                    nsketch++;
                    start = index + 1;
                    break;
                }
                // char* buffer = new char[index - start + 1];
                // memcpy(buffer, pattern + start, index - start);
                // buffer[index - start] = '\0';
                //SysDebug("start: %d, index: %d, buffer: %s\n", start, index, buffer);
                //SysDebug("sketches: %ud\n", sketches[nsketch]);
                
            }
            if(pattern[index] == '<'){
                lPos = index;
            }
            if(pattern[index] == '>'){
                rPos = index;
            }
            if(nsketch == 0 && lPos >= 0 && rPos > lPos){
                if(lPos != 0 && lPos > lastPos){ //Const field
                    fieldMode[fieldNum] = CONSTFIELD;
                    int tmp;
                    fieldType[fieldNum] = getStrSketch(pattern + lastPos, lPos - lastPos, NULL, tmp);
                    //if(varName == 14287360) printf("varName: 14287360, lPos: %d, lastPos: %d\n", lPos, lastPos);
                    fieldLen[fieldNum] = lPos - lastPos;
                    fieldNum++;
                }
                char fixlen_mark;
                int strTag_mark;
                int maxlen_mark;
                int scanRst = sscanf(pattern + lPos, "<%c,%d,%d>", &fixlen_mark, &strTag_mark, &maxlen_mark);
                if(scanRst == 3){
                    if(fixlen_mark == 'F'){
                        fieldMode[fieldNum] = FIXFIELD;
                        fieldType[fieldNum] = strTag_mark;
                        subvarType[subvarCnt++] = strTag_mark;
                        fieldLen[fieldNum] = maxlen_mark;
                    }else if(fixlen_mark == 'V'){
                        fieldMode[fieldNum] = VARFIELD;
                        fieldType[fieldNum] = strTag_mark;
                        subvarType[subvarCnt++] = strTag_mark;
                        fieldLen[fieldNum] = maxlen_mark;
                    }
                    fieldNum++;
                    lastPos = rPos + 1;
                }
                lPos = -1;
            }
        }

        if(lastPos < index){
            //printf("nsketch: %d, fieldNum: %d\n", nsketch, fieldNum);
            fieldMode[fieldNum] = CONSTFIELD;
            int tmp;
            fieldType[fieldNum] = getStrSketch(pattern + lastPos, index - lastPos, NULL, tmp);
            fieldLen[fieldNum] = index - lastPos;
            fieldNum++;
        }
        
        index++;

        for(;index < pLen; index++){
            if(pattern[index] == ' '){
                sketches[nsketch] = getPatSketch(pattern + start, index - start, varTypes, varCnt[nsketch], totVar);
                //if(varName == 14354432) printf("start: %d, index: %d, nsketch: %d, varCnt[nsketch]:%d\n", start, index, nsketch, varCnt[nsketch]);
                nsketch++;
                start = index + 1;
            }
        }
        if(start < index){
            sketches[nsketch] = getPatSketch(pattern + start, index - start, varTypes, varCnt[nsketch], totVar);
            nsketch++;
        }

        tvarLen = new int*[sketchNum];
        for(int t = 0; t < sketchNum; t++){
            tvarLen[t] = new int[varCnt[t]];
        }

        capsuleNum = subvarCnt + sketchNum;
        sketchSize = new int[sketchNum];
        for(int i = 0; i < sketchNum; i++) sketchSize[i] = 0;

        delete [] buffer;
    }
    if(type == DICPATTERN){ //DICT
        int start = 0, index = 0, nsketch = 0;
        sketchNum = 0;
        int pLen = strlen(pattern);
        char* buffer = new char[pLen];
        for(index = 0; index < pLen; index++){
            if(pattern[index] == ' '){
                if(sketchNum == 0){
                    memcpy(buffer, pattern + start, index - start);
                    buffer[index - start] = '\0';
                    //printf("start: %d, index: %d, buffer: %s\n", start, index, buffer);
                    sketchNum = atoi(buffer);
                    sketches = new unsigned int[sketchNum + 5];
                    varTypes = new unsigned int[sketchNum * MAX_VARIABLE];
                    totVar = 0;
                    varCnt = new int[sketchNum + 5];
                    for(int i = 0; i < sketchNum; i++) varCnt[i] = 0;
                    start = index + 1;
                    continue;
                }
                // char* buffer = new char[index - start + 1];
                // memcpy(buffer, pattern + start, index - start);
                // buffer[index - start] = '\0';
                //SysDebug("start: %d, index: %d, buffer: %s\n", start, index, buffer);
                sketches[nsketch] = getPatSketch(pattern + start, index - start, varTypes, varCnt[nsketch], totVar);
                //SysDebug("sketches: %ud\n", sketches[nsketch]);
                nsketch++;
                start = index + 1;
            }
        }
        if(start < index){
            sketches[nsketch] = getPatSketch(pattern + start, index - start, varTypes, varCnt[nsketch], totVar);
            nsketch++;
        }
        tvarLen = new int*[sketchNum];
        for(int t = 0; t < sketchNum; t++){
            tvarLen[t] = new int[varCnt[t]];
        }
        capsuleNum = sketchNum + 1;
        delete [] buffer;
    }
}

RuntimePattern::~RuntimePattern(){
    if(type == SUBPATTERN){
        if(fieldMode) {
            delete[] fieldMode;
            fieldMode = NULL;
        }
        if(fieldType) {
            delete[] fieldType;
            fieldType = NULL;
        }
        if(fieldLen) {
            delete[] fieldLen;
            fieldLen = NULL;
        }
    }
    if(type == DICPATTERN){
        if(sketches) {
            delete[] sketches;
            sketches = NULL;
        }
        if(varCnt) {
            delete[] varCnt;
            varCnt = NULL;
        }
        if(varTypes) {
            delete[] varTypes;
            varTypes = NULL;
        }
    }
}

int RuntimePattern::getDictNum(){
    return sketchNum;
}

string RuntimePattern::output(){
    string out = "";
    if(type == SUBPATTERN){
        out += "S : ";
        out += to_string(fieldNum) + " ";
        for(int t = 0; t < fieldNum; t++){
            if(fieldMode[t] == CONSTFIELD) out += "<Const," + to_string(fieldLen[t]) + ">";
            if(fieldMode[t] == FIXFIELD) out += "<Fix," + to_string(fieldType[t]) + "," + to_string(fieldLen[t]) + ">";
            if(fieldMode[t] == VARFIELD) out += "<Var," + to_string(fieldType[t]) + "," + to_string(fieldLen[t]) + ">";
        }
    }
    
    if(type == DICPATTERN){
        out += "D " + to_string(sketchNum) + " : sketches: ";
        for(int t = 0; t < sketchNum; t++){
            out += to_string(sketches[t]) + "d ";
        }
        int nVar = 0;
        for(int i = 0; i < sketchNum; i++){
            for(int t = 0; t < varCnt[i]; t++){
                out += "<V," + to_string(varTypes[nVar + t]) + ">";
            }
            nVar += varCnt[i];
            out += "| ";
        }
    }
    
    if(type == MULPATTERN){
        out += "M " + to_string(sketchNum) + " : sketches: ";
        for(int t = 0; t < sketchNum; t++){
            out += to_string(sketches[t]) + "d ";
        }
        out += to_string(fieldNum) + " ";
        for(int t = 0; t < fieldNum; t++){
            if(fieldMode[t] == CONSTFIELD) out += "<Const," + to_string(fieldLen[t]) + ">";
            if(fieldMode[t] == FIXFIELD) out += "<Fix," + to_string(fieldType[t]) + "," + to_string(fieldLen[t]) + ">";
            if(fieldMode[t] == VARFIELD) out += "<Var," + to_string(fieldType[t]) + "," + to_string(fieldLen[t]) + ">";
        }
        out += "| ";
        int nVar = 0;
        for(int i = 1; i < sketchNum; i++){
            for(int t = 0; t < varCnt[i]; t++){
                out += "<V," + to_string(varTypes[nVar + t]) + ">";
            }
            nVar += varCnt[i];
            out += "varCnt: " + to_string(varCnt[i]) + "| ";
        }
    }

    return out;
}

int RuntimePattern::cmpConst(int idx, int offset, int len, char* mbuf, int nowStart){
    if(offset + len - nowStart < fieldLen[idx]) return FAIL_RUNTIME_CONST_LEN;
    int tmp;
    if(getStrSketch(mbuf + nowStart, fieldLen[idx], NULL, tmp) != fieldType[idx]) return FAIL_RUNTIME_CONST;
    return 0;
}
int RuntimePattern::cmpFix(int idx, int offset, int len, char* mbuf, int nowStart){
    if(offset + len - nowStart < fieldLen[idx]) {
        //SysDebug("len: %d, offset: %d, nowStart: %d, varName: %d\n", len, offset, nowStart, varName); 
        return FAIL_RUNTIME_FIX_LEN;
    }
    unsigned int n_sketch = getType(mbuf + nowStart, fieldLen[idx]);
    if((n_sketch & fieldType[idx]) != n_sketch) {
        //SysDebug("varName: %d, n_sketch: %d, fieldType: %d\n", varName, n_sketch, fieldType[i]); 
        return FAIL_RUNTIME_FIX;
    }
    return 0;
}
int RuntimePattern::cmpVar(int idx, int offset, int len, char* mbuf, int nowStart){
    int n_len = 0;
    unsigned int n_typ = 0;
    for(int i = nowStart; (i - offset < len); i++){
        n_len++;
        n_typ |= getTypeC((int)*(mbuf + i));
    }
    if((n_typ & fieldType[idx]) != n_typ){
        return FAIL_RUNTIME_VAR;
    }
    if(n_len > fieldLen[idx]){
        return FAIL_RUNTIME_VAR_LEN;
    }
    // if(((n_typ & fieldType[sketch][idx]) != n_typ) || n_len > fieldLen[sketch][idx]) {
    //     //SysDebug("n_typ: %d, fieldType[i]: %d, n_len: %d, fieldLen[i]: %d", n_typ, fieldType[i], n_len, fieldLen[i]); 
    //     return -9;
    // }
    return n_len;
}

int RuntimePattern::match(int varName, char* mbuf, int offset, int len, int* l_mem, int& nGroup, int & l_pointer){
    // if(nGroup == -1){
    //     int tmp;
    //     unsigned int nHash = getValSketch(mbuf + offset, len, NULL, NULL, tmp);
    //     for(int i = 0; i < sketchNum; i++){
    //         if(nHash == sketches[i]) nGroup = i;
    //     }
    //     if(nGroup == -1) return FAIL_SUBVAR_NOGROUP;
    // }
    if(type == MULPATTERN){
        int n_varCnt = 0;
        unsigned int nHash = getValSketch(mbuf + offset, len, NULL, NULL, n_varCnt);
        if(nHash == sketches[0]){
            int nowStart = offset;
            int nowVar = 0;
            int ret = 0;
            for(int i = 0; i < fieldNum; i++){
                if(fieldMode[i] == CONSTFIELD){
                    if((ret = cmpConst(i, offset, len, mbuf, nowStart)) < 0) return ret;
                    nowStart += fieldLen[i];
                }
            
                if(fieldMode[i] == FIXFIELD){
                    if((ret = cmpFix(i, offset, len, mbuf, nowStart)) < 0) return ret;
                    int cofferName = varName | (nowVar << POS_SUBVAR) | VAR_TYPE_SUB;
                    
                    l_mem[l_pointer++] = nowStart;
                    l_mem[l_pointer++] = fieldLen[i];
                    l_mem[l_pointer++] = cofferName;
                    // g_offset[n_pointer] = nowStart;
                    // g_len[n_pointer] = fieldLen[i];
                    // g_pointer[n_pointer] = cofferName;
                    // n_pointer++;
                    // //if(n_pointer == TESTP) printf("MF: g_len[%d]: %d\n", n_pointer, g_len[n_pointer]);
                    
                    nowVar++;
                    nowStart += fieldLen[i];
                }

                if(fieldMode[i] == VARFIELD){
                    if((ret = cmpVar(i, offset, len, mbuf, nowStart)) < 0) return ret;
                    int cofferName = varName | (nowVar << POS_SUBVAR) | VAR_TYPE_SUB;
                    
                    l_mem[l_pointer++] = nowStart;
                    l_mem[l_pointer++] = ret;
                    l_mem[l_pointer++] = cofferName;
                    // g_offset[n_pointer] = nowStart;
                    // g_len[n_pointer] = ret;
                    // g_pointer[n_pointer] = cofferName;
                    // n_pointer++;
                    // //if(n_pointer == TESTP) printf("MV: g_len[30608]: %d\n", g_len[n_pointer]);
                    
                    nowVar++;
                    nowStart += ret;
                }
            }
            if(nowStart != offset + len) return FAIL_RUNTIME_LEN;
            return MATCHONMULP;
        }else{
            int n_var = 0;
            for(int i = 1; i < sketchNum; i++){
                // //SysDebug("varName: %d, sketches[i]: %ud\n", varName, sketches[i]); 
                if(nHash == sketches[i] && n_varCnt == varCnt[i]){
                    bool varMatch = true;
                    for(int j = 0; j < n_varCnt; j++){
                        if((n_varTypes[j] & varTypes[n_var]) != n_varTypes[j]){
                            //if(varName == 458752) printf("varName: %d, n_varTypes[j]: %ud, varTypes[n_var]: %ud\n", varName, n_varTypes[j], varTypes[n_var]); 
                            varMatch = false;
                        } 
                        n_var++;
                    }
                    if(varMatch){//Match on dictionary
                        nGroup = i;
                        l_mem[l_pointer++] = offset;
                        l_mem[l_pointer++] = len;
                        l_mem[l_pointer++] = varName;
                        // g_offset[n_pointer] = offset;
                        // g_len[n_pointer] = len;
                        // //if(n_pointer == TESTP) printf("MD: g_len[%d]: %d\n", n_pointer, g_len[n_pointer]);
                        // g_pointer[n_pointer] = varName;
                        //n_pointer++;
                        return MATCHONMULD;
                    }else{
                        n_var += varCnt[i];
                    }
                }
            }
            // char* tmp = new char[len+1];
            // memcpy(tmp, mbuf + offset, len);
            // tmp[len] = '\0';
            
            
            nGroup = sketchNum - 1;
            l_mem[l_pointer++] = offset;
            l_mem[l_pointer++] = len;
            l_mem[l_pointer++] = varName;

            return MATCHONMULD;
            //Dictionary outlier
            //return -11;
            //return FAIL_DICVAR_NOFOUND;
        }
    }
    if(type == SUBPATTERN){
        int nowStart = offset;
        int nowVar = 0;
        int ret = 0;
        for(int i = 0; i < fieldNum; i++){
            if(fieldMode[i] == CONSTFIELD){
                if((ret = cmpConst(i, offset, len, mbuf, nowStart)) < 0) return ret;
                nowStart += fieldLen[i];
            }
            
            if(fieldMode[i] == FIXFIELD){
                if((ret = cmpFix(i, offset, len, mbuf, nowStart)) < 0) return ret;
                int cofferName = varName | (nowVar << POS_SUBVAR) | VAR_TYPE_SUB;
                l_mem[l_pointer++] = nowStart;
                l_mem[l_pointer++] = fieldLen[i];
                l_mem[l_pointer++] = cofferName;
                nowVar++;
                nowStart += fieldLen[i];
            }

            if(fieldMode[i] == VARFIELD){
                if((ret = cmpVar(i, offset, len, mbuf, nowStart)) < 0) return ret;
                int cofferName = varName | (nowVar << POS_SUBVAR) | VAR_TYPE_SUB;
                l_mem[l_pointer++] = nowStart;
                l_mem[l_pointer++] = ret;
                l_mem[l_pointer++] = cofferName;
                nowVar++;
                nowStart += ret;
            }
        }
        if(nowStart != offset + len) return FAIL_RUNTIME_LEN;
        return MATCHONSUB;
    }else{ //Dictionary
        int n_varCnt = 0;
        unsigned int patSketch = getValSketch(mbuf + offset, len, n_varTypes, NULL, n_varCnt);
        int n_var = 0;
        //if(varName == 14287616) printf("14287616 skech: %ud, n_varCnt: %d\n", patSketch, n_varCnt);
        for(int i = 0; i < sketchNum; i++){
            // //SysDebug("varName: %d, sketches[i]: %ud\n", varName, sketches[i]); 
            if(patSketch == sketches[i] && n_varCnt == varCnt[i]){
                bool varMatch = true;
                for(int j = 0; j < n_varCnt; j++){
                    if((n_varTypes[j] & varTypes[n_var]) != n_varTypes[j]){
                        //if(varName == 458752) printf("varName: %d, n_varTypes[j]: %ud, varTypes[n_var]: %ud\n", varName, n_varTypes[j], varTypes[n_var]); 
                        varMatch = false;
                    } 
                    n_var++;
                }
                if(varMatch){
                    l_mem[l_pointer++] = offset;
                    l_mem[l_pointer++] = len;
                    l_mem[l_pointer++] = varName;
                    nGroup = i;
                    //printf("AddDPattern: l_pointer: %d, varName: %d, sn_offset: %d, sn_len: %d, nGroup: %d\n",l_pointer, varName, offset, len, nGroup);
                    return MATCHONDIC;
                }
            }else{
                n_var += varCnt[i];
            }
        }
        l_mem[l_pointer++] = offset;
        l_mem[l_pointer++] = len;
        l_mem[l_pointer++] = varName;
        nGroup = sketchNum - 1;
        //printf("AddDPattern: l_pointer: %d, varName: %d, sn_offset: %d, sn_len: %d, nGroup: %d\n",l_pointer, varName, offset, len, nGroup);
        return MATCHONDIC;
    }
}

unsigned int RuntimePattern::getFieldType(int idx){
    return subvarType[idx];
}

int RuntimePattern::getFieldLen(int idx){
    if(fieldMode[idx] == VARFIELD) return -1;
    return fieldLen[idx];
}