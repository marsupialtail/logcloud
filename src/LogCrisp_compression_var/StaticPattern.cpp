#include <iostream>
#include <string>
#include <set>
#include <cstring>
#include <assert.h>
#include <vector>
#include <cstdio>
#include "constant.h"
#include "StaticPattern.h"
#include "util.h"
#include "Coffer.h"

using namespace std;

StaticPattern::StaticPattern(int eid, const char* content, int pLen, const char * delim, int& nlen){
    
    Eid = eid;
    eid = (eid << POS_TEMPLATE);
    segCnt = 0;
    dliPos = new int[pLen];
    cnstPos = new int[pLen];
    varPos = new int[pLen];

    bool DELIM[128] = {false};
	int delimLen = strlen(delim);
    for(int i = 0; i < delimLen; i++){
        DELIM[int(delim[i])] = true;
    }

    segContent = new unsigned int[pLen];
    dliCnt = 0, cnstCnt = 0, varCnt = 0;
    int lastPos = 0;
    int tmp;

    for(int i = 0; i < pLen; i++){
        //printf("%d %d %d\n", i, content[i], DELIM[int(content[i])]);
        //if(content[i] < 0 || content[i] > 128) printf("Eid: %d, i: %d, pLen: %d, content[i]: %c\n", Eid, i, pLen, content[i]);
        if(content[i] < 0 || content[i] > 128) continue;
        if(DELIM[int(content[i])]){
            if(i > lastPos){
                int varNum = 0;
                int scanRst = sscanf(content + lastPos, "<V%d>", &varNum);
                if(scanRst == 1){
                    varPos[varCnt++] = segCnt;
                    segContent[segCnt++] = eid | (varNum << POS_VAR);

                }else{
                    cnstPos[cnstCnt++] = segCnt;
                    segContent[segCnt++] = getStrSketch(content + lastPos, i - lastPos, NULL, tmp);
                }
            }
            dliPos[dliCnt++] = segCnt;
            segContent[segCnt++] = int(content[i]);
            lastPos = i + 1;
        }
    }
    if(pLen > lastPos){
        int varNum = 0;
        int scanRst = sscanf(content + lastPos, "<V%d>", &varNum);
        if(scanRst == 1){
            varPos[varCnt++] = segCnt;
            segContent[segCnt++] = eid | (varNum << POS_VAR);
        }else{
            cnstPos[cnstCnt++] = segCnt;
            segContent[segCnt++] = getStrSketch(content + lastPos, pLen - lastPos, NULL, tmp);
        }
    }
    nlen = segCnt;
    RetArray = new int[varCnt];
    MatchType = new int[varCnt];
}

StaticPattern::~StaticPattern(){
    if(dliPos) {
        delete[] dliPos;
        dliPos = NULL;
    }
    if(cnstPos) {
        delete[] cnstPos;
        cnstPos = NULL;
    }
    if(varPos) {
        delete[] varPos;
        varPos = NULL;
    }
    if(segContent) {
        delete[] segContent;
        segContent = NULL;
    }
}

string StaticPattern::output(){
    string out = "" + to_string(segCnt) + "|";
    for(int i = 0; i < segCnt; i++){
        out += to_string(segContent[i]) + " ";
    }
    out += "|";
    for(int i = 0; i < dliCnt; i++){
        out += to_string(dliPos[i]) + " ";
    }
    out += "|";
    for(int i = 0; i < cnstCnt; i++){
        out += to_string(cnstPos[i]) + " ";
    }
    out += "|";
    for(int i = 0; i < varCnt; i++){
        out += to_string(varPos[i]) + " ";
    }
    out += "|";

    return out;
}
int StaticPattern::matchOnDelim(char * mbuf, int * offset, int * len){
    int ret = 0;
    for(int i = 0; i < dliCnt; i++){
        int n_pos = dliPos[i];
        int n_offset = offset[n_pos];
        int n_len = len[n_pos];
        if(n_len != 1) {
            // char* tmp = new char[505];
            // memcpy(tmp, mbuf + offset[0], 499);
            // tmp[500] = '\0';
            // printf("Eid: %d, return -2: %s\n", Eid, tmp);
            ret = FAIL_STATIC_DELIM_LEN;
            return ret;
        }
        unsigned int ndel = *(mbuf + n_offset);
        if(ndel != segContent[n_pos]) {
            // char* tmp = new char[500];
            // memcpy(tmp, mbuf + offset[0], 499);
            // tmp[500] = '\0';
            // printf("Eid: %d, ndel: %u, segContent: %u\n", Eid, ndel, segContent[n_pos]);
            ret = FAIL_STATIC_DELIM;
            //printf("here failed\n");
            return ret;
        }
    }
    return ret;
}

int StaticPattern::matchOnConstant(char* mbuf, int* offset, int * len){
    int ret = 0;
    for(int i = 0; i < cnstCnt; i++){
        int n_pos = cnstPos[i];
        int n_offset = offset[cnstPos[i]];
        int n_len = len[n_pos];
        int tmp;
        if(getStrSketch(mbuf + n_offset, n_len, NULL, tmp) != segContent[n_pos]) {
            ret = FAIL_STATIC_CONST;
            return ret;
        }
    }
    return ret;
}

int StaticPattern::matchOnVariable(char* mbuf, int *offset, int* len, int* st_start, int* l_mem, RuntimePattern** rp_store, int & l_pointer){
    int ret = 0;
    for(int i = 0; i < varCnt; i++){
        int n_pos = varPos[i];
        int n_offset = offset[n_pos];
        int n_len = len[n_pos];

        int varName = segContent[n_pos];
        int templateId = varName >> POS_TEMPLATE;
        int varId = (varName >> POS_VAR) & MASK_VAR;
        int var_pos = st_start[templateId] + varId;
        int org_pointer = l_pointer;
        int nGroup = -1;

        ret = rp_store[var_pos] ->match(varName, mbuf, n_offset, n_len, l_mem, nGroup, l_pointer);
        //SysDebug("varName: %d, org_pointer: %d, n_pointer: %d\n", varName, org_pointer, n_pointer);
        //if(varName == 262912) printf("varName: %d, ret: %d, org_pointer: %d, n_pointer: %d, nowLine: %d\n", varName, ret, org_pointer, n_pointer, nowLine);
        // if(Eid == 189){
        //     printf("Start match on i: %d, var: %d, ret: %d\n", i, varName, ret);
        // }
        if(l_pointer - org_pointer > MAX_SUBVARIABLE * 3){ //Too many subvariable
            ret = FAIL_SUBVAR_OVERFLOW;
        }

        if(ret < 0){ //match failed    
            l_pointer = 0;
            return ret;
        }
        //printf("E%d_V%d: ret: %d\n", templateId, varId, ret);
        if(ret == MATCHONSUB){
            MatchType[i] = MATCHONSUB;
            RetArray[i] = -(l_pointer - org_pointer);
        }else if(ret == MATCHONMULP){
            MatchType[i] = MATCHONMULP;
            RetArray[i] = -(l_pointer - org_pointer);
        }else if(ret == MATCHONMULD){
            MatchType[i] = MATCHONMULD;
            RetArray[i] = nGroup;
        }else{ //Match On Dic
            MatchType[i] = MATCHONDIC;
            RetArray[i] = nGroup;
        }
    }
    return ret;
}

void StaticPattern::updateMDPatternDic(char* mbuf, int& now_pointer, int nGroup, int nOffset, int nLen, int sketchNum, int * now_varLen, int* g_mem, int dic_pos, int dic_varName, Coffer** cp_store){
    int d_varCnt = 0;
    if(nGroup == sketchNum - 1){ //outlier
        d_varCnt = 1;
    }else{
        getValSketch(mbuf + nOffset, nLen, NULL, now_varLen, d_varCnt);
    }

    if(cp_store[dic_pos] == NULL){
        cp_store[dic_pos] = new Coffer(dic_varName);
        if(nGroup == sketchNum - 1){
            cp_store[dic_pos] ->dVarCnt = 1;
            cp_store[dic_pos] ->dVarLen = new int[1];
            cp_store[dic_pos] -> dVarLen[0] = nLen;
        }else{
            cp_store[dic_pos] ->dVarCnt = d_varCnt;
            cp_store[dic_pos] ->dVarLen = new int[d_varCnt];
            for(int v = 0; v < d_varCnt; v++) cp_store[dic_pos] -> dVarLen[v] = now_varLen[v];
        }
            //if(varName == 66560) printf("add to cp: %d\n", dic_varName);
        cp_store[dic_pos] ->startIdx = now_pointer;
        cp_store[dic_pos] ->endIdx = now_pointer;
        cp_store[dic_pos] ->eleLen = nLen;
        cp_store[dic_pos] ->lines = 0;
    }

    int pos = cp_store[dic_pos] ->lines % 7;
    if(pos == 0){
        g_mem[cp_store[dic_pos] ->endIdx] = now_pointer;
        cp_store[dic_pos] ->endIdx = now_pointer;
        g_mem[cp_store[dic_pos] ->endIdx + 1] = nOffset;
        g_mem[cp_store[dic_pos] ->endIdx + 2] = nLen;
        now_pointer += 16;
        //printf("In updateMDPatternDic Now pointer update to %d\n", now_pointer);
    }else{
        g_mem[cp_store[dic_pos] ->endIdx + pos * 2 + 1] =  nOffset;
        g_mem[cp_store[dic_pos] ->endIdx + pos * 2 + 2] =  nLen;
    }
    cp_store[dic_pos] ->eleLen = max(cp_store[dic_pos] ->eleLen, nLen);
    cp_store[dic_pos] ->lines++;
    if(nGroup == sketchNum - 1){ //outlier
                //printf("dic_pos: %d, var_start: %d, var_end: %d, nGroup: %d, storeName: %d, dicName: %d, d_varCnt: %d, dVarCnt: %d\n",dic_pos, rp_start[var_pos], rp_start[var_pos + 1], nGroup, cp_store[dic_pos] ->varName, dic_varName, d_varCnt, cp_store[dic_pos]->dVarCnt);
        cp_store[dic_pos] ->dVarLen[0] = max(cp_store[dic_pos] ->dVarLen[0], nLen);
    }else{
        assert(d_varCnt == cp_store[dic_pos] ->dVarCnt);
        for(int v = 0; v < d_varCnt; v++) cp_store[dic_pos] ->dVarLen[v] = max(cp_store[dic_pos] ->dVarLen[v], now_varLen[v]);                    
    }
}

void StaticPattern::updateMDPatternIdx(int& now_pointer, int nGroup, int* g_mem, RuntimePattern** rp_store, int * rp_start, int var_pos, int subvarCnt, int idx_varName, Coffer** cp_store){
    for(int idx_pos = rp_start[var_pos]; idx_pos < rp_start[var_pos] + subvarCnt; idx_pos++){
        if(cp_store[idx_pos] == NULL){
            int coffer_name = idx_varName | ((idx_pos - rp_start[var_pos]) << POS_SUBVAR);
            cp_store[idx_pos] = new Coffer(coffer_name);
            int dicCnt = rp_store[var_pos] ->sketchNum;
            cp_store[idx_pos] ->dicCnt = dicCnt;
            cp_store[idx_pos] ->dicSize = new int[dicCnt];
            for(int s = 0; s < dicCnt; s++) cp_store[idx_pos] ->dicSize[s] = 0;
                // cp_store[idx_pos] ->groupStart = new int[dicCnt];
                // for(int s = 0; s < dicCnt; s++) cp_store[idx_pos] ->groupStart[s] = -1;
            cp_store[idx_pos] ->fieldType = rp_store[var_pos] ->getFieldType(idx_pos - rp_start[var_pos]);
            cp_store[idx_pos] ->fieldLen = rp_store[var_pos] ->getFieldLen(idx_pos - rp_start[var_pos]);
            cp_store[idx_pos] ->startIdx = now_pointer;
            cp_store[idx_pos] ->endIdx = now_pointer;
            cp_store[idx_pos] ->eleLen = -1;
            cp_store[idx_pos] ->lines = 0;
        }
        int pos = cp_store[idx_pos] ->lines % 7;
        int value =  -((nGroup << 25) | (cp_store[idx_pos] ->dicSize[nGroup]));
        if(pos == 0){
            g_mem[cp_store[idx_pos] ->endIdx] = now_pointer;
            cp_store[idx_pos] ->endIdx = now_pointer;
            g_mem[cp_store[idx_pos] ->endIdx + 1] = value;
            g_mem[cp_store[idx_pos] ->endIdx + 2] = value;
            now_pointer += 16;
            //printf("In updateMDPatternIdx Now pointer update to %d\n", now_pointer);
        }else{
            g_mem[cp_store[idx_pos] ->endIdx + pos * 2 + 1] = value;
            g_mem[cp_store[idx_pos] ->endIdx + pos * 2 + 2] = value;
        }
        int tStrLen = 5;
        cp_store[idx_pos] ->eleLen = max(cp_store[idx_pos] ->eleLen, tStrLen);
        cp_store[idx_pos] ->lines++;
        cp_store[idx_pos] ->dicSize[nGroup]++;
        cp_store[idx_pos] ->outlierCnt++;
    }
}

void StaticPattern::updateMDPattern(char* mbuf, int& now_pointer, int& l_pointer, int nGroup, int * g_mem, int * l_mem, int* st_start, RuntimePattern** rp_store, int* rp_start, Coffer** cp_store){
    int nOffset = l_mem[l_pointer++]; //nGroup
    int nLen = l_mem[l_pointer++]; //dictionary value
    int varName = l_mem[l_pointer++]; //Dictionary Value

    int templateId = varName >> POS_TEMPLATE;
    int varId = (varName >> POS_VAR) & MASK_VAR;

    int idx_varName = varName | VAR_TYPE_SUB; //E3_V2~0.svar as index
    int dic_varName = varName | (nGroup << POS_SUBVAR) | VAR_TYPE_DIC;
    
    int var_pos = st_start[templateId] + varId;
    RuntimePattern* nowRp = rp_store[var_pos];
    nowRp ->totSize++;
    int subvarCnt = nowRp ->subvarCnt;
    int dic_pos = rp_start[var_pos] + subvarCnt + nGroup;
    int sketchNum = nowRp ->sketchNum;
    int * d_varLen = nowRp ->tvarLen[nGroup];
    //Dictionary Capsule
    updateMDPatternDic(mbuf, now_pointer, nGroup, nOffset, nLen, sketchNum, d_varLen, g_mem, dic_pos, dic_varName, cp_store);
    //Idx Capsule
    updateMDPatternIdx(now_pointer, nGroup, g_mem, rp_store, rp_start, var_pos, subvarCnt, idx_varName, cp_store);
}

void StaticPattern::updateSPattern(int& now_pointer, int& l_pointer, int ret, int typ, int* g_mem, int* l_mem, int* st_start, RuntimePattern** rp_store, int * rp_start, Coffer** cp_store){
    ret = -ret;
    //printf("in %d\n", now_pointer);
    // if(now_pointer > 3000000){
        
    // }
    while(ret){
        int sn_offset = l_mem[l_pointer++];
        int sn_len = l_mem[l_pointer++];
        int n_capName = l_mem[l_pointer++];
        int templateId = n_capName >> POS_TEMPLATE;
        int varId = (n_capName >> POS_VAR) & MASK_VAR;
        int svarId = (n_capName >> POS_SUBVAR) & MASK_SUBVAR;
        int var_pos = st_start[templateId] + varId;
        int n_pos = rp_start[var_pos] + svarId;
        //printf("sn_offset: %d, sn_len: %d, n_capName: %d, templateId: %d, varId: %d, svarId: %d\n", sn_offset, sn_len, n_capName, templateId, varId, svarId);
                //if(now_pointer == 150758) printf("j: %d, varCnt: %d, ret: %d, RetArray[j]: %d, Eid: %d, n_varName: %d\n", j, varCnt, ret, RetArray[j], Eid, n_varName);
        if(cp_store[n_pos] == NULL){
            cp_store[n_pos] = new Coffer(n_capName);
            if(typ == MATCHONMULP){
                int dicCnt = rp_store[var_pos] ->sketchNum;
                cp_store[n_pos] ->dicCnt = dicCnt;
                cp_store[n_pos] ->dicSize = new int[dicCnt];
                for(int s = 0; s < dicCnt; s++) cp_store[n_pos] ->dicSize[s] = 0;
            }
            cp_store[n_pos] ->fieldType = rp_store[var_pos] ->getFieldType(svarId);
            cp_store[n_pos] ->fieldLen = rp_store[var_pos] ->getFieldLen(svarId);
            cp_store[n_pos] ->startIdx = now_pointer;
            cp_store[n_pos] ->endIdx = now_pointer;
            cp_store[n_pos] ->lines = 0;
            cp_store[n_pos] ->eleLen = sn_len;
        }

        int pos = cp_store[n_pos] ->lines % 7;

        if(pos == 0){
            //if(n_capName == 196609) printf("196609 Add new block part! lines: %d, now_pointer: %d, value: %d, sn_offset: %d, base_offset: %d, sn_len: %d\n", cp_store[n_pos] ->lines, now_pointer, (sn_offset - cp_store[n_pos] ->baseOffset) << 10 | sn_len, sn_offset,cp_store[n_pos] ->baseOffset,sn_len);
            g_mem[cp_store[n_pos] ->endIdx] = now_pointer;
            cp_store[n_pos] ->endIdx = now_pointer;
            g_mem[cp_store[n_pos] ->endIdx + 1] = sn_offset;
            g_mem[cp_store[n_pos] ->endIdx + 2] = sn_len;
            now_pointer += 16;
            //printf("up to %d\n", now_pointer);
        }else{
            g_mem[cp_store[n_pos] ->endIdx + pos * 2 + 1] = sn_offset;
            g_mem[cp_store[n_pos] ->endIdx + pos * 2 + 2] = sn_len;
        }
         
        cp_store[n_pos] ->eleLen = max(cp_store[n_pos] ->eleLen, sn_len);
        cp_store[n_pos] ->lines++;
        ret -= 3;
    }
}

bool StaticPattern::updateDPatternEntry(int ret, int* g_entry, int& e_pointer, int ent_varName, int ent_pos, int sketchNum, unsigned int strSketch, Coffer** cp_store){
    bool add = false;
    if(cp_store[ent_pos] == NULL){
        cp_store[ent_pos] = new Coffer(ent_varName);
        cp_store[ent_pos] -> eleLen = 1;
                    
        cp_store[ent_pos] -> dicCnt = sketchNum; 
        
        cp_store[ent_pos] -> dicSize = new int[sketchNum];
        for(int s = 0; s < sketchNum; s++) cp_store[ent_pos] -> dicSize[s] = 0;

        int value = (ret << 25) | (cp_store[ent_pos] -> dicSize[ret]);
        cp_store[ent_pos] ->lines = 1;
        cp_store[ent_pos] -> startIdx_e = e_pointer;
        cp_store[ent_pos] -> endIdx_e = e_pointer;
    
        g_entry[e_pointer + 1] = value;
        e_pointer += 16;
        
        cp_store[ent_pos] -> addInDic(strSketch, value);
        cp_store[ent_pos] -> dicSize[ret]++;
        add = true;
    }else{
        int nowRet = cp_store[ent_pos] ->checkInDic(strSketch);
        if(nowRet == -1){ //Can not find
            nowRet = (ret << 25) | (cp_store[ent_pos] ->dicSize[ret]);
            cp_store[ent_pos] -> addInDic(strSketch, nowRet);
            if(cp_store[ent_pos] ->getDicSize() >255) cp_store[ent_pos] ->eleLen = 4;
            cp_store[ent_pos] -> dicSize[ret]++;
            add = true;
        }else{
            add = false;
        }
        int pos = cp_store[ent_pos] ->lines % 15;
        if(pos == 0){
            g_entry[cp_store[ent_pos] -> endIdx_e] = e_pointer;
            cp_store[ent_pos] -> endIdx_e = e_pointer;
            g_entry[e_pointer + 1] = nowRet;
            e_pointer += 16;                    
        }else{
            g_entry[cp_store[ent_pos] -> endIdx_e + pos + 1] = nowRet;
        }
        //if(cp_store[ent_pos] ->varName == 65539) printf("sketchNum: %d, dicSize[%d]: %d, nowRet: %d, g_entry[%d]\n", sketchNum, ret, cp_store[ent_pos] ->dicSize[ret], nowRet, cp_store[ent_pos] -> endIdx_e + pos + 1);
        
        cp_store[ent_pos] ->lines++;
    }
    return add;
}

void StaticPattern::updateDPatternDic(int& now_pointer, int nGroup, int offset, int len, int * g_mem,  int dic_varName, int dic_pos, int sketchNum, int * d_varLen, int d_varCnt, Coffer** cp_store){
    if(cp_store[dic_pos] == NULL){
        //if(dic_varName == 65536) printf("now pointer: %d\n", now_pointer);
        cp_store[dic_pos] = new Coffer(dic_varName);
        if(nGroup == sketchNum - 1){
            cp_store[dic_pos] -> dVarCnt = 1;
            cp_store[dic_pos] -> dVarLen = new int[1];
            cp_store[dic_pos] -> dVarLen[0] = len;
        }else{
            cp_store[dic_pos] ->dVarCnt = d_varCnt;
            cp_store[dic_pos] ->dVarLen = new int[d_varCnt];
            for(int v = 0; v < d_varCnt; v++) cp_store[dic_pos] -> dVarLen[v] = d_varLen[v];
        }        
        cp_store[dic_pos] ->startIdx = now_pointer;
        cp_store[dic_pos] ->endIdx = now_pointer;
        cp_store[dic_pos] ->eleLen = len;
        cp_store[dic_pos] ->lines = 0;
    }
    //printf("Update Dictionary liens: %d\n", cp_store[dic_pos] ->lines);
    int pos = cp_store[dic_pos] ->lines % 7;
    if(pos == 0){
        g_mem[cp_store[dic_pos] ->endIdx] = now_pointer;
        cp_store[dic_pos] ->endIdx = now_pointer;
        g_mem[cp_store[dic_pos] ->endIdx + 1] = offset;
        g_mem[cp_store[dic_pos] ->endIdx + 2] = len;
        //if(dic_varName == 65536) printf("Add to g_mem[%d], len: %d\n", cp_store[dic_pos] ->endIdx + 2, len);
        now_pointer += 16;
        //printf("In updateDPatternDic Now pointer update to %d\n", now_pointer);
    }else{
        g_mem[cp_store[dic_pos] ->endIdx + pos * 2 + 1] = offset;
        g_mem[cp_store[dic_pos] ->endIdx + pos * 2 + 2] = len;
        //if(dic_varName == 65536) printf("Add to g_mem[%d], len: %d\n", cp_store[dic_pos] ->endIdx + pos * 2 + 2, len);
    }
    cp_store[dic_pos] ->eleLen = max(cp_store[dic_pos] ->eleLen, len);
    cp_store[dic_pos] ->lines++;
        
    if(nGroup == sketchNum - 1){
        cp_store[dic_pos] ->dVarLen[0] = max(cp_store[dic_pos] ->dVarLen[0], len);
    }else{
        assert(d_varCnt == cp_store[dic_pos] ->dVarCnt);
        for(int v = 0; v < d_varCnt; v++) cp_store[dic_pos] ->dVarLen[v] = max(cp_store[dic_pos] ->dVarLen[v], d_varLen[v]);
    }
}

void StaticPattern::updateDPattern(char* mbuf, int& now_pointer, int& l_pointer, int nGroup, int * g_mem, int * l_mem, int* g_entry, int& e_pointer, int* st_start, RuntimePattern** rp_store, int* rp_start, Coffer** cp_store){
    int n_offset = l_mem[l_pointer++];
    int n_len = l_mem[l_pointer++];
    int varName = l_mem[l_pointer++];

    int templateId = varName >> POS_TEMPLATE;
    int varId = (varName >> POS_VAR) & MASK_VAR;
    int ent_varName = varName | VAR_TYPE_ENTRY;
    int dic_varName = varName | (nGroup << POS_SUBVAR) | VAR_TYPE_DIC;
    //printf("UpdateDPattern: l_pointer: %d, now_pointer: %d, sn_offset: %d, sn_len: %d, n_capName: %d, templateId: %d, varId: %d, ent_varName: %d\n",l_pointer, now_pointer, n_offset, n_len, varName, templateId, varId, ent_varName);
        
    int var_pos = st_start[templateId] + varId;
    rp_store[var_pos] ->totSize++;
    int ent_pos = rp_start[var_pos] + 0; //The first position is entry
    int dic_pos = rp_start[var_pos] + nGroup + 1;

    int d_varCnt = 0;
    int * d_varLen = NULL;
    int sketchNum = rp_store[var_pos] ->sketchNum;
    if(nGroup == sketchNum - 1){ //outlier
        d_varLen = NULL;
    }else{
        d_varLen = rp_store[var_pos] ->tvarLen[nGroup];
    }

    unsigned int strSketch = getStrSketch(mbuf + n_offset, n_len, d_varLen, d_varCnt);

    bool add = updateDPatternEntry(nGroup, g_entry, e_pointer, ent_varName, ent_pos, sketchNum, strSketch, cp_store);

    if(add){
        updateDPatternDic(now_pointer, nGroup, n_offset, n_len, g_mem, dic_varName, dic_pos, sketchNum, d_varLen, d_varCnt, cp_store);
    }
}


void StaticPattern::updatePattern(char* mbuf, int& now_pointer, int * g_mem, int * l_mem, int* g_entry, int& e_pointer, int* st_start, RuntimePattern** rp_store, int * rp_start, Coffer** cp_store){
    
    int l_pointer = 0;
    for(int j = 0; j < varCnt; j++){
        //if(now_pointer == 3365744 || now_pointer == 101) printf("j %d %d\n", j, now_pointer);
        int typ = MatchType[j];
        int ret = RetArray[j];
            //if(before_var_pointer == 522) printf("typ: %d, ret: %d\n", typ, ret);
        //printf("Update typ:%d, ret: %d\n", typ, ret);
        if(typ == MATCHONSUB || typ == MATCHONMULP){ //On subvar
            updateSPattern(now_pointer, l_pointer, ret, typ, g_mem, l_mem, st_start, rp_store, rp_start, cp_store);
        }

        if(typ == MATCHONMULD){ //On multiple dictionary
            int nGroup = ret;
            updateMDPattern(mbuf, now_pointer, l_pointer, nGroup, g_mem, l_mem, st_start, rp_store, rp_start, cp_store);
        }

        if(typ == MATCHONDIC){//On dictionary
            int nGroup = ret;
            updateDPattern(mbuf, now_pointer, l_pointer, nGroup, g_mem, l_mem, g_entry, e_pointer, st_start, rp_store, rp_start, cp_store);
        }   
    }
}


int StaticPattern::match(char* mbuf, int * offset, int* len, int segSize, int* g_mem, int * l_mem, int * g_entry, int & n_pointer, int & e_pointer, RuntimePattern** rp_store, Coffer** cp_store, int* st_start, int* rp_start, int nowLine){
    int ret = 0;

    //match on delim
    ret = matchOnDelim(mbuf, offset, len);
    if(ret < 0) return ret;

    //match on constant
    ret = matchOnConstant(mbuf, offset, len);
    if(ret < 0) return ret;

    // //match on variable and extract
    int l_pointer = 0;
    ret = matchOnVariable(mbuf, offset, len, st_start, l_mem, rp_store, l_pointer);
    if(ret < 0) return ret;
   
    //Update variable
    updatePattern(mbuf, n_pointer, g_mem, l_mem, g_entry, e_pointer, st_start, rp_store, rp_start, cp_store);
    return Eid;
}