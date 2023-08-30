#include <iostream>
#include <fstream>
#include <unistd.h>
#include <string>
#include <set>
#include <ctime>
#include <string.h>
#include <algorithm>
#include <regex>
#include <stdio.h>
#include <cstdio>
#include <assert.h>
#include <map>
#include <cmath>
#include <vector>
#include <fcntl.h>
#include <zlib.h>
#include <zstd.h>
#include <sys/mman.h>
#include <bitset>
#include <sys/io.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <gperftools/profiler.h>

#include "RuntimePattern.h"
#include "StaticPattern.h"
#include "constant.h"
#include "util.h"
#include "Coffer.h"
#include "timer.h"

using namespace std;



// TODO: Optimize to array
// StaticPattern** StaticPatternArray; //Static Pattern Store
// int staticCnt; //Static Pattern Size
// int * runtimeStart; //Runtime start for each static pattern
// RuntimePattern** RuntimePatternArray; //Runtime Pattern Store
// int * cofferStart; //Coffer start for each runtime pattern
// Coffer** CofferArray; //Coffer Store
StaticPattern* st_store[MAX_TEMPLATE];
RuntimePattern* rp_store[MAX_TEMPLATE * 32];
Coffer* cf_store[MAX_TEMPLATE * 32 * 4];

int st_start[MAX_TEMPLATE];
int rp_start[MAX_TEMPLATE * 32];


// int g_offset[MAXCROSS];
// int g_len[MAXCROSS];
// int g_pointer[MAXCROSS];
// int g_entry_f[MAXCROSS];
// int g_entry_p[MAXCROSS];

int g_mem[MAXCROSS * 3];
int l_mem[MAXTEMP];
int g_entry[MAXCROSS];
int g_eid[MAXLINE];
int tmpOffset[MAXTEMP];
int tmpLen[MAXTEMP];

int n_pointer = 0;
int e_pointer = 0;
int segSize = 0;
int segStart = 0, lineStart = 0, failLine = 0, nowLine = 0;
//map<int, StaticPattern*> st_store; // Eid to StaticPattern
//map<int, RuntimePattern*> rp_store; // VarNumber to RuntimePattern
//map<int, Coffer*> cf_store; // VarNumber to Coffer
const char* delim = " \t:=,[]";
//const char* sdelim = (char*)" \t:=,[]\n";
int len_start[MAX_TEMPLATE];
int len_offset[MAX_TEMPLATE];
// map<int, int*> st_index; //Length index to Eid
// map<int, int> st_cnt; //Length count
int stlen_start[MAX_SEGLEN];
int stlen_cnt[MAX_SEGLEN];

//return length of file data, -1 or 0: open file failed 
int LoadFileToMem(const char *varname, char **mbuf)
{
	int fd = open(varname,O_RDONLY);
	int len =0;
	
	if(fd != -1)
	{
		len = lseek(fd,0,SEEK_END);
		if(len > 0) 
		{
			*mbuf = (char *)mmap(NULL,len,PROT_READ,MAP_PRIVATE,fd,0);
		}
	}
	close(fd);
	return len;
}

int loadStaticPattern(string st_path){
	char* mbuf = NULL;
	int len = LoadFileToMem(st_path.c_str(), &mbuf);
	int cnt = 0, index = 0, slim = 0, eid = 0, lagestEid = 0;
	char content[len];
	//build 
	char *p = mbuf;
	int llPos = 0, nPos = 0;
	map<int, int> lenPos;
	for(int i = 0; i < MAX_SEGLEN; i++) stlen_start[i] = -1;
	for(int i = 0; i < MAX_SEGLEN; i++) stlen_cnt[i] = -1;
	for(int i = 0; i < MAX_TEMPLATE; i++) st_store[i] = NULL;
	for(int i = 0; i < MAX_TEMPLATE; i++) st_start[i] = -1;
	while(*p != '\n'){
		if(*p == '<') llPos = nPos;
		if(*p == ' '){
			int len_mark, start_mark, cnt_mark;
			int scanRst = sscanf(mbuf + llPos, "<%d,%d,%d>", &len_mark, &start_mark, &cnt_mark);
			if(scanRst == 3){
				stlen_start[len_mark] = start_mark;
				stlen_cnt[len_mark] = cnt_mark;
				lenPos[len_mark] = 0;
			}
		}
		p++; nPos++;
	}
	p++;
	for (; *p && p - mbuf < len; p++){
		if (*p == '\n'){
			content[index] = '\0';
			if(cnt > 1){
				int nlength = 0; //How many segment
				//printf("E%d %s %c\n", eid, content, content[0]);
				StaticPattern* nst = new StaticPattern(eid, content, index, delim, nlength);
				if(nst ->varCnt <= MAX_VARIABLE){
					int st_pos = stlen_start[nlength] + lenPos[nlength];
					lenPos[nlength]++;
					st_store[st_pos] = nst;
					st_start[nst ->Eid] = nst ->varCnt;
					if(nst ->Eid > lagestEid) lagestEid = nst ->Eid;
					
				}
				//if(nst ->varCnt > 255) continue;
			}
			slim = 0;
			index = 0;
			cnt = 0;
			content[0] = '\0';
		}else if(*p == ' '){
			if(slim == 2){
				content[index++] = *p;
			}else if(slim == 1){
				slim++;
			}else if(slim == 0){
				index = 0;
				slim++;
			}
		}else{
			if(slim == 2){ //content
				content[index++] = *p;
			}
			if(slim == 1){ //count
				cnt = cnt * 10 + (*p - '0');
			}
			if(slim == 0){ //eid
				if(index == 0){ //E
					eid = 0;
					index++;
				}else{
					int tmp = *p - '0';
					if(tmp >= 0 && tmp <= 9){
						eid = eid * 10 + tmp;
					}
				}
			}
		}
	}
	return lagestEid;
}

int loadRuntimePattern(string rp_path){
    char* mbuf = NULL;
    int len = LoadFileToMem(rp_path.c_str(), &mbuf);
	int largestPos = -1;
	if (len <= 0){
        SysWarning("Read file failed!\n");
        return largestPos;
    }
	
    char content[3][MAX_VALUE_LEN] = {'\0'};
    int index = 0, slim = 0;
    for(char *p = mbuf; *p && p - mbuf < len; p++){
		if (*p == '\n'){ //a new line
            content[slim][index] = '\0';
            int varName = atoi(content[0]);
            int templateId = varName >> POS_TEMPLATE;
			int varId = (varName >> POS_VAR) & MASK_VAR;
			int nPos = st_start[templateId] + varId;
			//printf("nPos: %d, tempalteId: %d, varId: %d\n", nPos, templateId, varId);
			if(st_start[templateId] != -1){
				rp_store[nPos] = new RuntimePattern(varName, content[1][0], content[2]);
				rp_start[nPos] = rp_store[nPos] ->capsuleNum;
				largestPos = max(largestPos, nPos);
			}
            slim = 0;
            index = 0;
            content[0][0] = '\0';
            content[1][0] = '\0';
        }else if(*p == ' ' && slim < 2){
            content[slim][index] = '\0';
            index = 0;
            slim++;
        }else{
            content[slim][index++] = *p;
        }
    }
	return largestPos;
}

int match(char* mbuf, int* offset, int* len, int segSize, int * g_mem, int* l_mem, int * g_entry, int& n_pointer, int& e_pointer, int nowLine){
	
	int ret = -99;

	if(stlen_start[segSize] == -1){
		ret = FAIL_SEG_LEN;
		return ret;
	}else{
		for(int i = stlen_start[segSize]; i < stlen_start[segSize] + stlen_cnt[segSize]; i++){ //Check Static Pattern of this length
			// if(n_pointer == 3365744 || n_pointer == 101) {
        	// 	printf("u1 n_pointer: %d, i: %d, stlen_start[%d]: %d, strlen_cnt:%d\n", n_pointer, i, segSize, stlen_start[segSize], stlen_cnt[segSize]);
    		// }
			
			if(st_store[i] == NULL) continue;
			int eid = st_store[i] ->Eid;

			//ret = 0;
			// if(n_pointer == 3365744 || n_pointer == 101) {
        	// 	printf("u2 n_pointer: %d, i: %d, stlen_start[%d]: %d, strlen_cnt:%d\n", n_pointer, i, segSize, stlen_start[segSize], stlen_cnt[segSize]);
    		// }
			ret = st_store[i] ->match(mbuf, offset, len, segSize, g_mem, l_mem, g_entry, n_pointer, e_pointer, rp_store, cf_store, st_start, rp_start, nowLine);
			//if(nowLine == 55609) printf("ret: %d, eid: %d\n", ret, eid);
			//printf("Match on eid: %d, segSize: %d, start: %d, len: %d, i: %d\n", eid, segSize, stlen_start[segSize], stlen_cnt[segSize], i);
			if(ret >= 0){
			//	printf("Success\n");
				
				return eid;
			}else{
			//	printf("Fail of %d\n", ret);
				//printf("totCnt: %d, segSize: %d\n", totCnt, segSize);
				//printf("check eid: %d, ret: %d\n", eid, ret);
				//printf("failed ret: %d\n", ret);
				//if(ret == -11) printf("Try to match on eid: %d, Temp match failed at: %d\n", eid, ret);
			}
		}
	}

	//printf("failed ret: %d\n", ret);
	return ret;
}

void compress(string zip_path, string zip_mode, int cp_level, int largetCapsule, int capsuleCnt){
	FILE* zipFile = fopen(zip_path.c_str(), "w");
	if(zipFile == NULL){
		printf("Open zip file failed\n");
		return;
	}
	int nowOffset = 0;
    
	Byte* meta = new Byte[1000000];
	int metaLen = 0;
	//printf("cf_size: %d\n", cf_size);
	memcpy(meta, (Byte*)&(capsuleCnt), 4);
	metaLen += 4;
	
	for(int i = 0; i < largetCapsule; i++){
		if(cf_store[i] == NULL || cf_store[i] ->srcLen == 0) continue;
		//string fileName = "./test/" + to_string(iit->first);
		int destLen = cf_store[i] ->compress(cp_level);
		int varName = cf_store[i] ->varName;
		//if(varName == 66576) printf("store var 66576\n");
		memcpy(meta + metaLen, (Byte*)&varName, 4);
		metaLen += 4;
		
		Byte tag = 0;
		int destTag = (destLen >= 128) ? 1 : 0;
		int srcTag = (cf_store[i] -> srcLen >= 128) ? 1 : 0;
		int linTag = (cf_store[i] -> lines >= 128) ? 1 : 0;
		int outlierTag = (cf_store[i] -> outlierCnt > 0) ? 1 : 0;
		int optimizeTag = (cf_store[i] ->optimize) ? 1 : 0;
		int isIntTag = (cf_store[i] ->isInt) ? 1 : 0;
		
		tag |= cf_store[i] ->compressed << META_TAG_CMP;
		tag |= destTag << META_TAG_DST;
		tag |= srcTag << META_TAG_SRC;
		tag |= linTag << META_TAG_LIN;
		tag |= outlierTag << META_TAG_OUTLIER;
		tag |= optimizeTag << META_TAG_OPT;
		tag |= isIntTag << META_TAG_ISINT;

		meta[metaLen++] = tag;

		if(destTag){
			memcpy(meta + metaLen, (Byte*)&destLen, 4);
			metaLen += 4;
		}else{
			meta[metaLen++] = (Byte)destLen;
		}

		if(srcTag){
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->srcLen), 4);
			metaLen += 4;
		}else{
			meta[metaLen++] = (Byte)(cf_store[i] ->srcLen);
		}

		if(linTag){
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->lines), 4);
			metaLen += 4;
		}else{
			meta[metaLen++] = (Byte)(cf_store[i] ->lines);
		}

		memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->eleLen), 4);
		metaLen += 4;

		if(cf_store[i] ->type == VAR_TYPE_SUB){
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->org_eleLen), 4);
			metaLen += 4;
		}

		if(cf_store[i] -> type == VAR_TYPE_ENTRY){
			//int nowDicCnt = 0;
			//for(int i = 0; i < tcoffer ->dicCnt; i++) if(tcoffer ->dicSize[i] > 0) nowDicCnt++;
			//printf("nowDicCnt: %d, tcoffer ->groupCnt: %d, varName: %d ", nowDicCnt, tcoffer ->groupCnt, tcoffer ->varName & 0xfffffff0);
			//for(int i = 0; i < tcoffer ->dicCnt; i++) printf("%d ", tcoffer->dicSize[i]);
			//printf("\n");
			//assert(nowDicCnt == tcoffer ->groupCnt);
			//if(varName == 65795) printf("65795 dicCnt: %d\n", tcoffer ->dicCnt);

			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->dicCnt), 4);
			metaLen += 4;
			//meta[metaLen++] = (Byte)(tcoffer ->dicCnt + 1);
			
			for(int t = 0; t < cf_store[i] ->dicCnt; t++){
				//if(tcoffer ->dicSize[i] == 0) continue;
				memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->groupStart[t]), 4);
				metaLen += 4;
			}
		}
		
		if(cf_store[i] ->type == VAR_TYPE_DIC){
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->dVarCnt), 4);
			metaLen += 4;
			for(int v = 0; v < cf_store[i] ->dVarCnt; v++){
				memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->dVarLen[v]), 4);
				metaLen += 4;
			}
		}
		
		if(outlierTag){
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->dicCnt), 4);
			metaLen += 4;
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->outlierCnt), 4);
			metaLen += 4;
			//printf("Write Meta: %d, now_dicCnt: %d, now_outlierCnt: %d ",varName, cf_store[i] ->dicCnt, cf_store[i] ->outlierCnt);
			for(int t = 0; t < cf_store[i] ->dicCnt; t++){
				//printf("%d ", cf_store[i] ->groupStart[t]);
				memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->groupStart[t]), 4);
				metaLen += 4;
			}
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->intMin), 4);
			metaLen += 4;
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->intMax), 4);
			metaLen += 4;
			//printf("\n");
		}

		if(isIntTag && !outlierTag){
			meta[metaLen++] = cf_store[i] ->nowSeg;			
			for(int t = 0; t < cf_store[i] ->nowSeg; t++){
				meta[metaLen++] = cf_store[i] ->metaTag[t];
				memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->metaEnd[t]), 4);
				metaLen += 4;
				memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->metaMax[t]), 4);
				metaLen += 4;
				memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->metaMin[t]), 4);
				metaLen += 4;
			}
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->intMin), 4);
			metaLen += 4;
			memcpy(meta + metaLen, (Byte*)&(cf_store[i] ->intMax), 4);
			metaLen += 4;
		}
		//sprintf(tmp, "%u %d %d %d %d %d %d\0", iit ->first, tcoffer ->compressed,  nowOffset, destLen, tcoffer -> srcLen, tcoffer ->lines, tcoffer ->eleLen);
		//printf("tlen: %d\n", tlen);
		nowOffset += destLen;
	}
	meta[metaLen] = '\0';


	size_t com_space_size = ZSTD_compressBound(metaLen);
	Byte* pZstd = new Byte[com_space_size];
	size_t srcLen = metaLen;
	size_t destLen = ZSTD_compress(pZstd, com_space_size, meta, metaLen, cp_level);
	fwrite(&destLen, sizeof(size_t), 1, zipFile);
	fwrite(&srcLen, sizeof(size_t), 1, zipFile);
	fwrite(pZstd, sizeof(Byte), destLen, zipFile);
	delete [] pZstd;

	if(zip_mode != "Z" && zip_mode != "ZA"){
		FILE * test = fopen((zip_path + ".meta").c_str(), "w");
		map<int, Coffer*> c_map;
		c_map.clear();
		Coffer::readCoffer(meta, metaLen, c_map);
		string tmp = (string)"varName numName subvarName typeName " + "compressed " + " " + "destLen" + " " + "srcLen" + " " + "lines" + " " + "eleLen" + "\n";
		for(map<int, Coffer*>::iterator iit = c_map.begin(); iit != c_map.end(); iit++){
			tmp += iit ->second ->printMeta();
			//printf("varName: %d\n", iit->second->varName);
			//delete iit ->second;
		}

		fprintf(test, "%s", tmp.c_str());
		fclose(test);
	}

	// int destLen = meta_cf ->compress();
	// fwrite(&destLen, sizeof(int), 1, zipFile);
	// meta_cf ->output(zipFile);

	for(int i = 0; i < largetCapsule; i++){
		if(cf_store[i] == NULL || cf_store[i] ->srcLen == 0) continue;
		cf_store[i] ->output(zipFile);
	}

	fclose(zipFile);
	delete [] meta;
}

void matchToken(int pos, int& maxSegLen){
	if(pos > segStart){
		tmpOffset[segSize] = segStart;
		tmpLen[segSize] = pos - segStart;
		if(tmpLen[segSize] > maxSegLen) maxSegLen = tmpLen[segSize];
		segSize++;
	}
	tmpOffset[segSize] = pos;
	tmpLen[segSize] = 1;
	segStart = pos + 1;
	segSize++;
}


void matchLine(char* mbuf, int pos, int * eidCount, int& maxSegSize, string output_path){
	if(pos > segStart){
		tmpOffset[segSize] = segStart;
		tmpLen[segSize] = pos - segStart;
		segSize++;
	}
	
	int eid = match(mbuf, tmpOffset, tmpLen, segSize, g_mem, l_mem, g_entry, n_pointer, e_pointer, nowLine);

	if(eid < 0){ //match failed
				
				// char* tmp = new char[i - lineStart + 1];
            	// memcpy(tmp, mbuf + lineStart, i - lineStart);
            	// tmp[i - lineStart] = '\0';
				// SysDebug("Final match failed at %d, segSize: %d, buf: %s\n\n", eid, segSize, tmp);
				//if(eid == -11) totDicOutlier++;
		printf("Match Failed: %d\n", eid);
		
		FILE* fout = fopen((output_path + ".outlier").c_str(), "a");
		char* buffer = new char[pos - lineStart + 5];
		memcpy(buffer, mbuf + lineStart, pos - lineStart);
		buffer[pos-lineStart] = '\0';
		fprintf(fout, "%s\n", buffer);
		fclose(fout);
		
		g_eid[nowLine] = eid;
		cf_store[VAR_TYPE_OUTLIER] ->addData(mbuf + lineStart, pos - lineStart);
		failLine++;
	}else{
		eidCount[eid]++;
		//SysDebug("Success Matched at %d\n\n", eid);
		g_eid[nowLine] = eid;
	}
	nowLine++;
	maxSegSize = max(maxSegSize, segSize);
	segSize = 0;
	segStart = pos + 1;
	lineStart = pos + 1;
}

void matchFile(char* mbuf, int len, int* eidCount, int& maxSegSize, int& maxSegLen, string outputPath){
	register int DELIM[4];
	for(int i = 0; i < 4; i++) DELIM[i] = 0;

	int delimLen = strlen(delim);
    for(int i = 0; i < delimLen; i++){
		int ret = int(delim[i]) / 32;
		int offset = int(delim[i]) - 32 * ret;
		DELIM[ret] |= (1 << offset);
		//printf("int(delim[i]): %d, DELIM: %d\n", int(delim[i]), DELIM[int(delim[i])]);
    }
	for(int i = 0; i < len; i++){
		int tint = int(mbuf[i]);
		int ret = tint / 32;
		int offset = tint - 32 * ret;
		if(tint == 10){
			matchLine(mbuf, i, eidCount, maxSegSize, outputPath);
		}else if((DELIM[ret] >> offset) & 1){
			matchToken(i, maxSegLen);
		}
	}
}

void process(int largestCapsule, string now_input_path, string now_output_path, int* eidCount, int compress_level, string zip_mode){
	char* mbuf = NULL;

	for(int i = 0; i < largestCapsule; i++){
		cf_store[i] = NULL;
	}

	printf("Start to process: %s, output path: %s \n", now_input_path.c_str(), now_output_path.c_str());
	int len = LoadFileToMem(now_input_path.c_str(), &mbuf);
	if(len <= 0){
		printf("Load input file failed: %s\n", now_input_path.c_str());
		return;
	}

		
	n_pointer = 0, e_pointer = 0, segSize = 0, segStart = 0, lineStart = 0, failLine = 0, nowLine = 0;
	cf_store[VAR_TYPE_OUTLIER] = new Coffer(VAR_TYPE_OUTLIER, len);
	memset(eidCount, 0, sizeof(int) * (MAX_TEMPLATE));
	
	printf("Start match!\n");
	timeval mtime_s = ___StatTime_Start();
	int maxSegSize = -1;
	int maxSegLen = -1;

	matchFile(mbuf, len, eidCount, maxSegSize, maxSegLen,now_output_path);

	printf("Max SegSize: %d, Max SegLen: %d\n", maxSegSize, maxSegLen);
	double stime = ___StatTime_End(mtime_s);

	printf("Match Failed num: %d, Rate: %f, time: %fs\n", failLine, (double(failLine)) / nowLine, stime);
	// FILE * fo = fopen("./matchRes.txt", "a+");
	// fprintf(fo, "%s: Match Failed num: %d, Rate: %f, time: %fs\n", now_input_path.c_str(), failLine, (double(failLine)) / nowLine, stime);
	// fclose(fo);
		//printf("Dictionary Outlier Failed num: %d, Rate: %f\n", totDicOutlier, (double(totDicOutlier)) / failLine);
	
		//Eid
	if(zip_mode != "Z" && zip_mode != "ZA"){
		FILE* eid_out = fopen((now_output_path + ".eid").c_str(), "w");
		for(int i = 0; i < nowLine; i++){
			fprintf(eid_out, "%d\n", g_eid[i]);
		}
		fclose(eid_out);
	}
		//cf_store[VAR_TYPE_EID] = new Coffer(VAR_TYPE_EID, nowLine);
		//Coffer* c_eid = cf_store[VAR_TYPE_EID];
		//for(int i = 0; i < nowLine; i++){
		//c_eid ->entry[i] = g_eid[i];
		////printf("geid: %d\n", g_eid[i]);
		// }
		// c_eid ->lines = nowLine;
	
	

		//Coffer copy and compress

	printf("Start copy, n_pointer: %d, e_pointer: %d, now line: %d!\n", n_pointer, e_pointer, nowLine);
	timeval cptime_s = ___StatTime_Start();
	int capsuleCount = 0;
	for(int i = 0; i < largestCapsule; i++){
		if(cf_store[i] == NULL) continue;
		int Eid = (cf_store[i] ->varName >> POS_TEMPLATE);
		cf_store[i] ->copy(mbuf, g_mem, g_entry, eidCount[Eid]);
		if(cf_store[i] ->srcLen > 0) capsuleCount++;
	}
	double cptime = ___StatTime_End(cptime_s);
	printf("Copy cost: %fs\n", cptime);

	// if(zip_mode != "Z" && zip_mode != "ZA"){
	// 	string dirName = now_output_path + "/";
	// 	if(access(dirName.c_str(), 0) == -1){
	// 		mkdir(dirName.c_str(), S_IRWXU);
	// 	}
	// 	for(int i = 0; i < largestCapsule; i++){
	// 		if(cf_store[i] == NULL || cf_store[i] ->srcLen == 0) continue;
	// 		string fileName = dirName + to_string(cf_store[i] ->varName);
	// 		FILE* fp = fopen(fileName.c_str(), "w");
	// 		fprintf(fp, "%s", (cf_store[i] ->print()).c_str());
	// 		fclose(fp);
	// 	}
	// }

		//printf("cf_size: %d, %d\n", cf_store.size(), (cf_store.find(805974016) == cf_store.end()));
    	//4). Compression Capsule and write
	printf("Start compress!\n");
	timeval ctime_s = ___StatTime_Start();
	compress(now_output_path + ".zst", zip_mode,  compress_level, largestCapsule, capsuleCount);
	double ctime = ___StatTime_End(ctime_s);
	printf("Compress cost: %fs\n", ctime);
		// for(auto iit: st_store){
		// 	delete iit.second;
		// }
		// for(auto iit: rp_store){
		// 	delete iit.second;
		// }
		// for(auto iit: cf_store){
		// 	//printf("varName: %d\n", iit.second->varName);
		// 	//delete iit.second;
		// }
}

// ./Compressor -I ../Hadoop_0.log -O ../Hadoop_0 -T ../LogTemplate/Hadoop
int main(int argc, char *argv[]){

	// clock_t start = clock();
	int o;
	const char *optstring = "HhI:O:T:Z:L:X:Y:";
	srand(4);
	//Input Content
	string input_path; string output_path;
    string template_path;
	string compression_level_org;
	int compression_level;
	string zip_mode;
	int startFile = -1;
	int endFile = -1;

    //Input A.log -> A.zst use
	while ((o = getopt(argc, argv, optstring)) != -1)
	{
		switch (o)
		{
		case 'I':
			input_path = optarg;
			//strcpy(input,optarg);
			printf("input file path: %s\n", input_path.c_str()); //clove++ 20200919: path
			break;
		case 'O':
			output_path = optarg;
			printf("output path : %s\n", output_path.c_str());
			break;
		case 'T':
			template_path = optarg;
			printf("template path: %s\n", template_path.c_str());
			break;
		case 'L':
			compression_level_org = optarg;
			printf("compression level org: %s\n", compression_level_org.c_str());
			break;
        case 'Z':
			zip_mode = optarg;
			printf("zip mode: %s\n", zip_mode.c_str());
			break;
		case 'X':
			startFile = atoi(optarg);
			printf("start from: %d.log\n", startFile);
			break;
		case 'Y':
			endFile = atoi(optarg);
			printf("end at: %d.log\n", endFile);
			break;
		case 'h':
		case 'H':
			printf("-I input path\n");
			printf("-O output path\n");
			printf("-T template path\n");
			printf("-L compression_level\n");
			printf("-Z compression_mode(Z or O)\n");
            return 0;
			break;
		case '?':
			printf("error:wrong opt!\n");
			printf("error optopt: %c\n", optopt);
			printf("error opterr: %c\n", opterr);
			return 1;
		}
	}
	
	//Basic input check
	if (input_path == ""){
		printf("No input file use default ../Hadoop_0.log\n");
		input_path = "../Hadoop_0.log";
	}
	
	if (output_path == ""){
		printf("No output use default ../Hadoop_0\n");
		output_path = "../Hadoop_0";
	}
    if (template_path == ""){
        printf("No template use default ../LogTemplate/Hadoop_n\n");
		template_path = "../LogTemplate/Hadoop";
    }
	
	if (compression_level_org == ""){
		compression_level = 10;
	}else{
		compression_level = atoi(compression_level_org.c_str());
	}
	if (zip_mode == ""){
		zip_mode = "O";
	}
	if (zip_mode == "ZA"){
		if(startFile == -1 || endFile == -1){
			printf("error: No file range\n");
			return -1;
		}
	}


	ProfilerStart("LogCrisp.prof");
    string staticPattern_path = template_path + ".templates";
    string runtimePattern_path = template_path + ".variables";
	string tag_path = template_path + ".tags";

	//TODO:
	//1). Load staticPattern -> staticPatternTree
    int largestEid = loadStaticPattern(staticPattern_path);
	int largestRm = 0;
	
	// for(int i = 1; i <= largestEid; i++){
	// 	printf("i: %d, st_cnt[i]: %d\n", i, st_start[i]);
	// }
	for(int i = 0; i <= largestEid; i++){
		if(st_start[i] == -1) continue;
		int st_cnt = st_start[i];
		st_start[i] = largestRm;
		//printf("i: %d, st_start[i]: %d\n", i, st_start[i]);
		largestRm += st_cnt;
	}
	//Output static pattern
	
	if(zip_mode != "Z" && zip_mode != "ZA"){
		FILE* sout = fopen("./sta.txt", "w");
		for(int i = 0; i < MAX_TEMPLATE; i++){
			if(st_store[i] == NULL) continue;
			fprintf(sout, "E%d %s\n", st_store[i] ->Eid, (st_store[i]->output()).c_str());
		}
		fclose(sout);
	}


    //2). Load runtimePattern -> patternMap(int -> <runtimtPattern>)
    int largestRuntimePos = loadRuntimePattern(runtimePattern_path);
	largestRuntimePos++;
	int largestCpausle = 8;
	for(int i = 0; i < largestRuntimePos; i++){
		if(rp_store[i] == NULL) continue;
		int rp_cnt = rp_start[i];
		rp_start[i] = largestCpausle;
		largestCpausle += rp_cnt;
	}

	//Output runtime pattern
	if(zip_mode != "Z" && zip_mode != "ZA"){
		FILE* tout = fopen("./var.txt", "w");
		for(int i = 0; i < largestRuntimePos; i++){
			if(rp_store[i] == NULL) continue;
			fprintf(tout, "%d %s\n", rp_store[i] ->varName, (rp_store[i]->output()).c_str());
		}
		fclose(tout);
	}

	string * input_path_array;
	string * output_path_array;
	int arraySize = 1;
	if(zip_mode != "ZA"){
		input_path_array = new string[1];
		output_path_array = new string[1];
		input_path_array[0] = input_path;
		output_path_array[0] = output_path;
	}else{
		input_path_array = new string[endFile - startFile];
		output_path_array = new string[endFile - startFile];
		arraySize = endFile - startFile;
		for(int i = startFile; i < endFile; i++){
			input_path_array[i - startFile] = input_path + to_string(i) + ".log";
			output_path_array[i - startFile] = output_path + to_string(i);
		}
	}
	
	int * eidCount = new int[MAX_TEMPLATE];
	//3). Matching and Building Capsule
	for(int ss = 0; ss < arraySize; ss++){
		process(largestCpausle, input_path_array[ss], output_path_array[ss], eidCount, compression_level, zip_mode);
	}

	// for(auto iit: st_index){
	// 	delete[] iit.second;
	// }
	ProfilerStop();
} 
