#include "Coffer.h"
#include "util.h"
#include<cstring>
#include<cstdio>
#include<cstdlib>
#include<assert.h>
#include<map>
#include <zstd.h>
using namespace std;

Coffer::Coffer(int _varName){
    startIdx = -1;
    endIdx = -1;
    startIdx_e = -1;
    endIdx_e = -1;
    dicStart = -1;

    varName = _varName;
    
    data = NULL;
    cdata = NULL;
    groupStart = NULL;
    dicSize = NULL;

    srcLen = 0;
    destLen = 0;
    eleLen = 0;
    type = _varName & 0xf;
    lines = 0;

    fieldType = 0;

    compressed = 0;
    optimize = false;
    isInt = false;
    offset = 0;
    outlierCnt = 0;

    intMin = -1;
    intMax = -1;
    mixPattern = false;
}   

Coffer::Coffer(int _varName, int size){
    startIdx = -1;
    endIdx = -1;
    
    varName = _varName;
    if(_varName == VAR_TYPE_OUTLIER){
        data = new Byte[size];
    }

    srcLen = 0;
    destLen = 0;
    eleLen = 0;
    type = _varName & 0xf;
    lines = 0;
    fieldType = 0;
    dicStart = -1;

    cdata = NULL;
    groupStart = NULL;
    dicSize = NULL;
    compressed = 0;
    optimize = false;
    isInt = false;
    offset = 0;
    outlierCnt = 0;

    intMin = -1;
    intMax = -1;
    mixPattern = false;
}   




// Coffer::Coffer(const char* metaStr){
//     data = NULL;
//     cdata = NULL;
//     type = -1;
//    // cout << "Build based: " << metaStr << endl;
//     int tLen = metaStr.size();
//     int nowStart = 0;
//     int nowCnt = 0;
//     char buf[1024];
//     for (int i = 0; i < tLen; i++){
//         if(metaStr[i] == ' '){
//             memcpy(buf, metaStr + nowStart, i - nowStart);
//             int value = 
//         }
//     }
//     //sscanf(metaStr.c_str(), "%d %d %d %d %d %d %d", varName, &_compressed, &_offset, &_destLen, &_srcLen, &_lines, &_eleLen);
//     //cout << filename << endl;
//     //cout << _compressed << endl;
//     //cout << _compressed << _offset << _destLen << _srcLen << _lines << _eleLen << endl;
//     type = (unsigned int)varName & 0xf;
//     compressed = _compressed;
//     offset = _offset;
//     destLen = _destLen;
//     srcLen = _srcLen;
//     lines = _lines;
//     eleLen = _eleLen;
// }

// Coffer::~Coffer()
// {
//     //printf("start\n");
//     if(data){
//         //delete[] data;
//         data = NULL;
//         //printf("here11\n");
//     }
//     if(cdata){
//         //delete[] cdata;
//         cdata = NULL;
//         //printf("here12\n");
//     }
//     if(dicSize){
//         //delete [] dicSize;
//         dicSize = NULL;
//         //printf("here13\n");
//     }
//     if(groupStart){
//         //delete [] groupStart;
//         groupStart = NULL;
//         //printf("here14\n");
//     }
// }

int Coffer::compress(int compression_level){
    //cout << "Here1 size: " << this -> lines << endl;
    if(srcLen == 0){
        destLen = 0;
        return destLen;
    }

    size_t com_space_size = ZSTD_compressBound(srcLen);
    cdata = new Byte[com_space_size];
    destLen = ZSTD_compress(cdata, com_space_size, data, srcLen, compression_level);

    if (destLen >= srcLen){
        destLen = srcLen;
        return destLen;
    }
    compressed = 1;
   // this -> lines = tempLines;
    //cout << "Here: " << this -> print() << endl;
    return destLen;
}

int Coffer::readFile(FILE* zipFile, int fstart, string compression_method){
    int totOffset = fstart + this->offset;
    fseek(zipFile, totOffset, SEEK_SET);
    //printf("Read At: %d\n", totOffset);
    cdata = new Byte[destLen + 5];
    return fread(cdata, sizeof(Byte), destLen, zipFile);
}

int Coffer::decompress(string compression_method){
    if(compressed == 0){
        if(srcLen == 0) return 0;
        data = new Byte[srcLen + 5];
        memcpy(data , cdata , sizeof(Byte)*srcLen);
    }else{
        size_t decom_buf_size = ZSTD_getFrameContentSize(cdata, destLen);
        data = new Byte[decom_buf_size + 5];
        int res = ZSTD_decompress(data, decom_buf_size, cdata, destLen);
        if(res != srcLen){
            printf("varName: %d decompresss failed with %d\n", varName, res);
            return -1;
        }
    }
        
    if(type == VAR_TYPE_ENTRY && optimize){
        int * nowMax = new int[dicCnt];
        for(int i = 0; i < dicCnt; i++) nowMax[i] = groupStart[i];
        int ptr = 0;
        while(ptr < srcLen){
            int tmp;
            memcpy((Byte*)&tmp, data + ptr, 4);
            if(tmp < 0){
                int ret = -tmp - 1;
                tmp = nowMax[ret];
                nowMax[ret]++;
                memcpy(data + ptr, (Byte*)&tmp, 4);
            }
            ptr += 4;
        }
    }

    if(outlierCnt > 0 && (((varName >> POS_SUBVAR) & MASK_SUBVAR) == 0)){
        //build index
        outlierIdx = new int[outlierCnt];
        int nLine = 0, ptr = 0;
        if(isInt){
            while(ptr < srcLen){
                int tmp;
                memcpy((Byte*)&tmp, data + ptr, 4);
                if(tmp < 0){
                    int ret = -tmp - 1;
                    outlierIdx[ret] = nLine;
                }
                ptr += 4;
                nLine++;
            }
        }else{
            while(ptr < srcLen){
                if(data[ptr + eleLen - 1] == 0xff){
                    int tmp;
                    memcpy((Byte*)&tmp, data + ptr + eleLen - 5, 4);
                    int ret = -tmp;
                    outlierIdx[ret] = nLine;
                }
                ptr += eleLen;
                nLine++;
            }
        }
    }
    return srcLen;
}

void Coffer::output(FILE* zipFile){
    if(zipFile == NULL){
        cout << "coffer: " + to_string(varName) + " output failed" << endl;
        return;
    }
    if(compressed){
        fwrite(cdata, sizeof(Byte), destLen, zipFile);
    }else{
        fwrite(data, sizeof(Byte), srcLen, zipFile);
    }
}

void Coffer::readCoffer(const Byte* meta, int mLen, map<int, Coffer*>& t_map){
    int tmp = 0;
    memcpy((Byte*)&tmp, meta, 4);
    //printf("tmp: %d\n", tmp);
    int pos = 4;
    int nowOffset = 0;
    for(int i = 0; i < tmp; i++){
        
        int varName;
        memcpy((Byte*)&varName, meta + pos, 4);
        pos += 4;
        //printf("varName: %d\n", varName);
        Coffer* n_coffer = new Coffer(varName);
        
        Byte tag = meta[pos++];
		int cmpTag = tag >> META_TAG_CMP & 1;
        int destTag = tag >> META_TAG_DST & 1;
        int srcTag = tag >> META_TAG_SRC & 1;
        int linTag = tag >> META_TAG_LIN & 1;
        bool outlierTag = tag >> META_TAG_OUTLIER & 1;
        bool optTag = tag >> META_TAG_OPT & 1;
        bool isIntTag = tag >> META_TAG_ISINT & 1;
        n_coffer ->compressed = cmpTag;
        n_coffer ->optimize = optTag;
        n_coffer ->isInt = isIntTag;

        if(destTag){
            memcpy((Byte*)&(n_coffer -> destLen), meta + pos, 4);
            pos += 4;
        }else{
            n_coffer -> destLen = meta[pos++];
        }

        if(srcTag){
            memcpy((Byte*)&(n_coffer -> srcLen), meta + pos, 4);
            pos += 4;
        }else{
            n_coffer -> srcLen = meta[pos++];
        }

        if(linTag){
            memcpy((Byte*)&(n_coffer -> lines), meta + pos, 4);
            pos += 4;
        }else{
            n_coffer -> lines = meta[pos++];
        }

        memcpy((Byte*)&(n_coffer ->eleLen), meta + pos, 4);
        pos += 4;

        if((varName & 0xf) == VAR_TYPE_SUB){
            memcpy((Byte*)&(n_coffer ->org_eleLen), meta + pos, 4);
            pos += 4;
        }
        //if(n_coffer ->lines != 0) n_coffer ->eleLen = n_coffer ->srcLen / n_coffer ->lines;

        if((varName & MASK_TYPE) == VAR_TYPE_ENTRY){
            memcpy((Byte*)&(n_coffer ->dicCnt), meta + pos, 4);
            pos += 4;
            n_coffer -> groupStart = new int[n_coffer ->dicCnt];
            for(int i = 0; i < n_coffer ->dicCnt; i++){
                memcpy((Byte*)&(n_coffer ->groupStart[i]), meta + pos, 4);
                pos += 4;
            }
        }

        if((varName & 0xf) == VAR_TYPE_DIC){
            memcpy((Byte*)&(n_coffer ->dVarCnt), meta + pos, 4);
            pos += 4;
            n_coffer ->dVarLen = new int[n_coffer ->dVarCnt];
            for(int v = 0; v < n_coffer ->dVarCnt; v++){
                memcpy((Byte*)&(n_coffer ->dVarLen[v]), meta + pos, 4);
                pos += 4;
            }
        }

        if(outlierTag){
            memcpy((Byte*)&(n_coffer ->dicCnt), meta + pos, 4);
            pos += 4;
            memcpy((Byte*)&(n_coffer ->outlierCnt), meta + pos, 4);
            pos += 4;
            //printf("Read Meta: %d, now_dicCnt: %d, now_outlierCnt: %d ",varName, n_coffer ->dicCnt, n_coffer ->outlierCnt);
            n_coffer ->groupStart = new int[n_coffer ->dicCnt];
            for(int i = 0; i < n_coffer ->dicCnt; i++){
                memcpy((Byte*)&(n_coffer ->groupStart[i]), meta + pos, 4);
                //printf("%d ", n_coffer ->groupStart[i]);
                pos += 4;
            }
            memcpy((Byte*)&(n_coffer ->intMin), meta + pos, 4);
			pos += 4;
			memcpy((Byte*)&(n_coffer ->intMax), meta + pos, 4);
			pos += 4;
            //printf("\n");
        }

        if(isIntTag && !outlierTag){
            n_coffer ->nowSeg = meta[pos++];
            n_coffer ->metaTag = new Byte[MAX_META_SEG];
            n_coffer ->metaEnd = new int[MAX_META_SEG];
            n_coffer ->metaMax = new int[MAX_META_SEG];
            n_coffer ->metaMin = new int[MAX_META_SEG];

            for(int i = 0; i < n_coffer ->nowSeg; i++){
                n_coffer ->metaTag[i] = meta[pos++];
                memcpy((Byte*)&(n_coffer ->metaEnd[i]), meta + pos, 4);
                pos += 4;
                memcpy((Byte*)&(n_coffer ->metaMax[i]), meta + pos, 4);
                pos += 4;
                memcpy((Byte*)&(n_coffer ->metaMin[i]), meta + pos, 4);
                pos += 4;
            }
            memcpy((Byte*)&(n_coffer ->intMin), meta + pos, 4);
            pos += 4;
            memcpy((Byte*)&(n_coffer ->intMax), meta + pos, 4);
            pos += 4;
        }

        n_coffer ->offset = nowOffset;
        nowOffset += n_coffer -> destLen;
        t_map[varName] = n_coffer;
    }

    for(map<int, Coffer*>::iterator iit = t_map.begin(); iit != t_map.end(); iit++){
        // if((iit ->first & 0xf) == VAR_TYPE_DIC){
            // printf("iit first: %d\n", iit ->first);
            // int now_ent = iit ->first & 0xfffff00;
            // if(GroupCnt.find(now_ent) == GroupCnt.end()) GroupCnt[now_ent] = 0;
            // iit ->second ->dicStart = t_map[now_ent | VAR_TYPE_ENTRY] ->groupStart[GroupCnt[now_ent]++];
            // for(int i = 0; i <= iit ->second ->groupCnt; i++){
            //     int now_dic = now_ent | (i << 4) | VAR_TYPE_DIC;
            //     if(t_map.find(now_dic) != t_map.end()){
            //         t_map[now_dic] ->dicStart = iit ->second ->groupStart[i];
            //     }
            // }
        // }
        if((iit ->first & 0xf) == VAR_TYPE_ENTRY){
            int now_var = iit ->first & MASK_VARNAME;
            int totCnt = iit ->second ->dicCnt;
            for(int i = 0; i < totCnt; i++){
                int nowStart = iit ->second ->groupStart[i];
                if(nowStart == -1) continue;
                int now_dic = now_var | (i << POS_SUBVAR) | VAR_TYPE_DIC;
                t_map[now_dic] ->dicStart = nowStart;
            }
        }
    }
    //Outlier dictionary
    for(map<int, Coffer*>::iterator iit = t_map.begin(); iit != t_map.end(); iit++){
        if((iit ->first & 0xf) == VAR_TYPE_DIC && iit ->second ->dicStart == -1){
            int now_var = iit ->first & MASK_VARNAME;
            int now_svar = now_var | VAR_TYPE_SUB;
            int now_group = (iit ->first >> POS_SUBVAR) & MASK_SUBVAR;
            //printf("Read: %d, now_var: %d, now_group: %d, group_start: %d\n", iit->first, now_var, now_group, t_map[now_svar] ->groupStart[now_group]);
            iit ->second ->dicStart = t_map[now_svar] ->groupStart[now_group];
        }
    }
}
string Coffer::print(){
    if(srcLen == 0){
        return "";
    }
    if(type == VAR_TYPE_ENTRY || isInt){
        string name = "";
        if(eleLen == 1){
            for(int i = 0; i < srcLen; i++){
                int tmp = data[i];
                name += to_string(tmp) + "\n";
            }
        }else{
            int nowStart = 0;
            while(nowStart < srcLen){
                int tmp;
                memcpy((Byte*)&tmp, data + nowStart, 4);
                nowStart += 4;
                name += to_string(tmp) + "\n";
            }
        }
        return name;
    }
    

    data[srcLen] = '\0';
    string name = string((char*)data);
    return name;
}
string Coffer::printMeta(){
    int templateName = (varName >> POS_TEMPLATE);
    int varvecName = (varName >> POS_VAR) & MASK_VAR;
    int patternName = (templateName << POS_TEMPLATE) + (varvecName << POS_VAR);
    int subvarName = (varName >> POS_SUBVAR) & MASK_SUBVAR;
    int typeName = varName & MASK_TYPE;
    string typeStr = typeNames[typeName];
    string tmp = "E" + to_string(templateName) + "_V" + to_string(varvecName) + "~" + to_string(subvarName) + " " + typeStr + " " + to_string(patternName) + " " + to_string(varName)  + " " + to_string(compressed) + " " + to_string(destLen) + " " + to_string(srcLen) + " " +to_string(lines) + " " + to_string(eleLen);
    //printf("here: varName %d, fieldType: %u\n", varName, this -> fieldType);
    if(type == VAR_TYPE_ENTRY){
        if(optimize){
            tmp += " optimized ";
        }else{
            tmp += " no-optimized ";
        }
        tmp += "(dicCnt: " + to_string(dicCnt) + " groupStart: ";
        for(int i = 0; i < dicCnt; i++){
            tmp += to_string(groupStart[i]) + " ";
        }
        tmp += ")";
    }
    if(type == VAR_TYPE_DIC){
        tmp += "(dicStart: " + to_string(dicStart) + " dVarCnt: " + to_string(dVarCnt) + " dVarLen: ";
        for(int i = 0; i < dVarCnt; i++){
            tmp += to_string(dVarLen[i]) + " ";
        }
        tmp += ")";
    }
    if(type == VAR_TYPE_SUB){
        if(isInt){
            tmp += " isInt ";
        }else{
            tmp += " notInt ";
        }
        if(outlierCnt > 0){
            tmp += "outlier: " + to_string(outlierCnt);
            tmp += " Min: " + to_string(intMin) + " Max: " + to_string(intMax);
        }else{
            if(isInt){
                tmp += "(origianlLength: " + to_string(org_eleLen) + " Min: " + to_string(intMin) + " Max: " + to_string(intMax) + " tSeg: " + to_string(nowSeg) + " segMeta: ";
                for(int i = 0; i < nowSeg; i++){
                    tmp += "<" + to_string(metaTag[i]) + ", end at: " + to_string(metaEnd[i]) + ", max: " + to_string(metaMax[i]) + ", min: " + to_string(metaMin[i]) + "> ";
                }
                tmp += ")";
            }
        }
    }
    tmp += "\n";
    return tmp;
}

string Coffer::print(char* mbuf, int * g_offset, int * g_len, int* g_pointer, int* g_entry, int* g_entry_p){
    string content = "";
    if(type == VAR_TYPE_ENTRY){
        int n_ep = startIdx_e;
        for(int i = 0; i < lines; i++){
            int now_entry_value = g_entry[n_ep];
            n_ep = g_entry_p[n_ep];
            content += to_string(now_entry_value) + "\n";
        }
    }else if(type == VAR_TYPE_OUTLIER){
        data[srcLen] = '\0';
        content = string((char*)data);
    }else{
        int n_cnt = 0;
        int n_pointer = startIdx;
        while(n_cnt != lines){
            int n_offset = g_offset[n_pointer];
            int n_len = g_len[n_pointer];
            Byte* tmp = new Byte[n_len + 1];
            memcpy(tmp, mbuf + n_offset, n_len);
            tmp[n_len] = '\0';
            content += string((char*)tmp) + "\n";
            n_cnt++;
            n_pointer = g_pointer[n_pointer];
        }
    }
    return content;
}

void Coffer::printFile(string output_path){
    FILE* pFile = fopen((output_path + to_string(varName)).c_str(), "w");
    //cout <<output_path + to_string(varName) <<endl;
	fprintf(pFile, "%s", (this -> print()).c_str());
	fclose(pFile);
}

int Coffer::checkInDic(unsigned int data){
    if(dataDic.find(data) != dataDic.end()){
        return dataDic[data];
    }else{
        return -1;
    }
}

void Coffer::addInDic(unsigned int data, int value){
    dataDic[data] = value;
}
int Coffer::getDicSize(){
    return dataDic.size();
}

void Coffer::addData(const char* start, int len){
    memcpy(data + srcLen, start, len);
    data[srcLen + len] = '\n';
    srcLen += len + 1;
    lines++;
}

void Coffer::clear(){
    srcLen = 0;
    destLen = 0;
    eleLen = 0;
    lines = 0;
    fieldType = 0;

    startIdx = -1;
    endIdx = -1;
    startIdx_e = -1;
    endIdx_e = -1;

    compressed = 0;
    optimize = false;
    isInt = false;
    offset = 0;

    
}

void Coffer::dataSet(string& _data){
    srcLen = _data.size();
    data = new Byte[srcLen+5];
    memcpy(data, _data.c_str(), srcLen);
    data[srcLen] = '\0';
}
void Coffer::dicInit(){
    data = new Byte[eleLen * lines+5];
    int nowStart = 0;
    groupStart = new int[dicCnt];
    for(int i = 0; i < dicCnt; i++){
        if(dicSize[i] == 0) {groupStart[i] = -1; continue;}
        groupStart[i] = nowStart;
        nowStart += dicSize[i];
    }
}
int Coffer::getIdx(int value){
    int ret = value >> 25;
    int offset = value & 0xffffff;
    return groupStart[ret] + offset;
}

void Coffer::copyEntry(char* mbuf, int * g_entry, int totSize){
    //lines = totSize;
    int nowOffset = 0;
    dicInit();
    int n_entry_p = startIdx_e;
    //if(varName == 395523) printf("395523 read block: %d\n", n_entry_p);
    int * nMax = new int[dicCnt];
    if(((int)dataDic.size() * 2 > lines) && eleLen == 4){
        for(int i = 0; i < dicCnt; i++){
            nMax[i] = 0;
        }
        optimize = true;
    } 
    for(int i = 0; i < lines; i++){
        int pos = i % 15;
        if(i != 0 && pos == 0){
            n_entry_p = g_entry[n_entry_p];
            //if(varName == 395523) printf("395523 read block: %d\n", n_entry_p);
        }
        int value = g_entry[n_entry_p + pos + 1];
        int ret = value >> 25;
        int offset = value & 0xffffff;
        //if(varName == 65539) printf("g_entry[%d]: %d, ret: %d, offset: %d\n", n_entry_p + pos + 1, value, ret, offset);
        int storeIdx = groupStart[ret] + offset;
        //if(varName == 525059) printf("525059 copy ret: %d, offset: %d, result: %d\n", ret, offset, storeIdx);
        //n_entry_p = g_entry_p[n_entry_p];
        if(optimize && storeIdx > nMax[ret]){
            nMax[ret] = storeIdx;
            storeIdx = -(ret + 1);
        }
        if(eleLen == 1){
            data[nowOffset++] = (Byte)storeIdx;
        }else{
            memcpy(data + nowOffset, (Byte*)&storeIdx, 4);
            nowOffset += 4;
        }
    }
    srcLen = eleLen*lines;
    assert(srcLen == nowOffset);
}

void Coffer::copyInteger(char* mbuf, int *g_mem, int totSize){
        int dataWidth = (eleLen <= 2) ? 1 : 4;
        int nowOffset = 0;
        isInt = true;
        data = new Byte[dataWidth * lines + 5];
        memset(data, 0, dataWidth * lines);
        int n_cnt = 0;
        int n_pointer = startIdx;
        if(outlierCnt > 0){ //Outlier int
            dataWidth = 4;
            int nowStart = 0;
            groupStart = new int[dicCnt];
            for(int i = 0; i < dicCnt; i++){
                if(dicSize[i] == 0) {groupStart[i] = -1; continue;}
                groupStart[i] = nowStart;
                nowStart += dicSize[i];
            }

            while(n_cnt != lines){
                int pos = n_cnt % 7;
                if(n_cnt != 0 && pos == 0){
                    n_pointer = g_mem[n_pointer];
                }
                int n_offset = g_mem[n_pointer + pos * 2 + 1];
                int n_len = g_mem[n_pointer + pos * 2 + 2];
                
                if(n_offset < 0){ //outlier
                    int value = -n_offset;
                    int n_Group = value >> 25;
                    int l_offset = value & 0xffffff;
                    int storeIdx = -(groupStart[n_Group] + 1 + l_offset);
                    //printf("StoreIdx: %d, n_Group: %d, groupStart: %d, l_offset: %d\n", storeIdx, n_Group, groupStart[n_Group], l_offset);
                    memcpy(data + nowOffset, (Byte*)&storeIdx, 4);
                    nowOffset += dataWidth;
                    n_cnt++;
                }else{
                    int temp = 0;
                    for(int idx = 0; idx < n_len; idx++) temp += (*(mbuf + n_offset + idx) - '0') * (BASE10[n_len - idx - 1]); //change to int
                    memcpy(data + nowOffset, (Byte*)&temp, 4);
                    if(temp > intMax) intMax = temp;
                    if(temp < intMin || intMin == -1) intMin = temp;
                    nowOffset += dataWidth;
                    n_cnt++;
                }
            }
            srcLen = dataWidth * lines;
            org_eleLen = (fieldLen == -1) ? -1 : eleLen;
            eleLen = dataWidth;
            assert(srcLen == nowOffset);
            return;
        }
        
        int lineThreash = lines / (MAX_META_SEG) + 1;
        metaEnd = new int[MAX_META_SEG + 5];
        metaMax = new int[MAX_META_SEG + 5];
        metaMin = new int[MAX_META_SEG + 5];
        metaTag = new Byte[MAX_META_SEG + 5];

        nowSeg = 0;
        int nowSegCnt = 0;
        int nowMax = 0;
        int nowMin = -1;
        int nowState = -1; // 1 shows increase range, 2 shows decrease range, 0 shows unknown
        int lastNum = -1;
        while(n_cnt != lines){

            if(nowSegCnt >= lineThreash){ //newSeg
                metaEnd[nowSeg] = n_cnt;
                metaMax[nowSeg] = nowMax;
                metaMin[nowSeg] = nowMin;
                metaTag[nowSeg] = nowState;

                nowSeg++;
                nowSegCnt = 0;
                nowMax = -1;
                nowMin = -1;
                nowState = -1;
                lastNum = -1;
            }
            int pos = n_cnt % 7;
            if(n_cnt != 0 && pos == 0){
                n_pointer = g_mem[n_pointer];
            }
            int n_offset = g_mem[n_pointer + pos * 2 + 1];
            int n_len = g_mem[n_pointer + pos * 2 + 2];
            int temp = 0;
            
            for(int idx = 0; idx < n_len; idx++) temp += (*(mbuf + n_offset + idx) - '0') * (BASE10[n_len - idx - 1]); //change to int
            
            //now Max, now Min
            if(temp > nowMax) nowMax = temp;
            if(nowMin == -1 || temp < nowMin) nowMin = temp;
            
            if(temp > intMax) intMax = temp;
            if(intMin == -1 || temp < intMin) intMin = temp;
            //now state
            if(lastNum == -1){
                lastNum = temp;
            }else if(nowState == -1){
                nowState = (temp >= lastNum) ? 1: 2;
                lastNum = temp;
            }else if(nowState == 1){
                nowState = (temp >= lastNum) ? 1: 0;
                lastNum = temp;
            }else if(nowState == 2){
                nowState = (temp < lastNum) ? 2: 0;
                lastNum = temp;
            }

            if(dataWidth == 1){
                data[n_cnt] = (Byte)temp;
            }else{
                memcpy(data + nowOffset, (Byte*)&temp, 4);
            }
            nowOffset += dataWidth;
            n_cnt++;
            nowSegCnt++;
        }

        if(nowSegCnt != 0){
            metaEnd[nowSeg] = n_cnt;
            metaMax[nowSeg] = nowMax;
            metaMin[nowSeg] = nowMin;
            metaTag[nowSeg] = nowState;
            nowSeg++;
        }

        srcLen = dataWidth * lines;
        org_eleLen = (fieldLen == -1) ? -1 : eleLen;
        eleLen = dataWidth;
        assert(srcLen == nowOffset);
}

void Coffer::copyVar(char* mbuf, int* g_mem, int totSize, int prefix){ //Dictionary, non-integer
        //printf("varName: %d, eleLen: %d, lines: %d\n", varName, eleLen, lines);
        data = new Byte[eleLen * lines + 5];
        memset(data, ' ', eleLen * lines); //Padding
        int n_cnt = 0;
        int n_pointer = startIdx;
        int nowOffset = 0;

        if(outlierCnt > 0){
            int nowStart = 0;
            groupStart = new int[dicCnt];
            for(int i = 0; i < dicCnt; i++){
                if(dicSize[i] == 0) {groupStart[i] = -1; continue;}
                groupStart[i] = nowStart;
                nowStart += dicSize[i];
            }
            while(n_cnt != lines){
                int pos = n_cnt % 7;
                if(n_cnt != 0 && pos == 0){
                    n_pointer = g_mem[n_pointer];
                }
                int n_offset = g_mem[n_pointer + pos * 2 + 1];
                int n_len = g_mem[n_pointer + pos * 2 + 2];
                if(n_offset < 0){ //outlier
                    int value = -n_offset;
                    int n_Group = value >> 25;
                    int l_offset = value & 0xffffff;
                    int storeIdx = -(groupStart[n_Group] + l_offset);
                    data[nowOffset + eleLen - 1] = 0xff;
                    memcpy(data + nowOffset + eleLen - 5, (Byte*)&storeIdx, 4);
                    for(int t = nowOffset; t < (nowOffset + eleLen - 5); t++){
                        data[t] = 0xff;
                    }
                }else{
                    int n_start = nowOffset + (eleLen - n_len);
                    memcpy(data + n_start, mbuf + n_offset, n_len);
                }
                nowOffset += eleLen;
                n_cnt++;
            }
            srcLen = eleLen * lines;
            assert(srcLen == nowOffset);
            return;
        }
        int templateID = varName >> POS_TEMPLATE;
        int varID = varName >> POS_VAR & 0xff;
        string fileName = "E" + to_string(templateID) + "_V" + to_string(varID);
        FILE* fw = fopen(("./variable_" + std::to_string(prefix) + "/" + fileName).c_str(), "w");
        string tagPath = "./variable_" + std::to_string(prefix) + "_tag.txt";
        FILE* ftag = fopen(tagPath.c_str(), "a");
        char* buffer = new char[1000000];
        //printf("varName: %d, eleLen: %d, lines: %d, n_pointer: %d, nowOffset: %d\n", varName, eleLen, lines, n_pointer, nowOffset);
        int tType = 0;
        while(n_cnt != lines){
            int pos = n_cnt % 7;
            if(n_cnt != 0 && pos == 0){
                n_pointer = g_mem[n_pointer];
            }
            int n_offset = g_mem[n_pointer + pos * 2 + 1];
            int n_len = g_mem[n_pointer + pos * 2 + 2];
            //printf("varName: %d, g_mem[%d], len: %d\n", varName, n_pointer + pos * 2 + 2, n_len);
            int n_start = nowOffset + (eleLen - n_len);
            //printf("varName: %d, eleLen: %d, lines: %d, n_pointer: %d, nowOffset: %d, offset: %d, len: %d\n", varName, eleLen, lines, n_pointer, nowOffset, n_offset, n_len);
            memcpy(buffer, mbuf + n_offset, n_len);
            buffer[n_len] = '\0';
            int nType = getType(buffer, n_len);
            tType |= nType;
            fprintf(fw, "%s\n", buffer);
            //if(varName == 198401) printf("n_pointer: %d, Copy Capsule %d: eleLen: %d, lines: %d, n_start: %d, n_offset: %d, n_len: %d\n", n_pointer, varName, eleLen, lines, n_start, n_offset, n_len);
            
            memcpy(data + n_start, mbuf + n_offset, n_len);
            nowOffset += eleLen;
            n_cnt++;
        }
        fprintf(ftag, "%s %d\n", fileName.c_str(), tType);
        fclose(fw);
        fclose(ftag);
        srcLen = eleLen * lines;
        assert(srcLen == nowOffset);
}

int Coffer::copy(char* mbuf, int* g_mem, int * g_entry,int totSize, int prefix){
    
    if(type == VAR_TYPE_ENTRY){
        copyEntry(mbuf, g_entry, totSize);
    }
    else if(type == VAR_TYPE_OUTLIER){
        return 0;
    }else if(type == VAR_TYPE_EID){
        return 0;
    }
    else if(fieldType == NUM_TY && eleLen < 10){ //INTEGER
        //printf("here varName: %d, fieldType: %u\n", varName, fieldType);
        copyInteger(mbuf, g_mem, totSize);
    }else{ //Common coffer, (svar, dic)
        copyVar(mbuf, g_mem, totSize, prefix);
    }
    return 0;
}
