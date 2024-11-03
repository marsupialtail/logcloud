#include <iostream>
#include <fstream>
#include <unistd.h>
#include <string>
#include <set>
#include <ctime>
#include <string.h>
#include <stdio.h>
#include <cstdio>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <map>
#include <cmath>
#include <vector>
#include <algorithm>


#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "LengthParser.h"
#include "util.h"
#include "timer.h"
#include "Group.h"
#include "template.h"
#include "constant.h"

using namespace std;

const char* delim = " \t:=,[]";
// const char * delim = " \t:=,[]<>.";
// const char* delim = " \t!#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~";

bool cmp(const pair<unsigned int, int>& p1, const pair<unsigned int, int>& p2){
    return p1.second > p2.second;
}
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

//clove rewrite 20220415,  use mmap
int matchFile(char* mbuf, int len, LengthParser* parser, map<int, VarArray*>& variables, int& nowLine)
{
    
    SegTag segArray[MAXTOCKEN];
    int segSize = 0;
    int segStart = 0; 
    int lineStart = 0;
    int failLine = 0;
    for (int i=0; i<len; i++)
	{
        if (mbuf[i] == '\n')
		{
            if(i > segStart)
            {
                segArray[segSize].tag = 0;
                segArray[segSize].startPos = segStart;
                segArray[segSize].segLen=i-segStart;
                segSize++;
            }
            int eid = parser -> SearchTemplate(mbuf, segArray, segSize, variables, true);
			if(eid == -1)
            {
                SysWarning("Miss match on training set\n");
			}
            nowLine++;
            segSize = 0;
            segStart = i + 1;
            lineStart = i + 1;
		}
        else if(strchr(delim,mbuf[i]))//记录分割符号
        {
            if(i > segStart)//delim前有字符串
            {
                segArray[segSize].tag = 0;
                segArray[segSize].startPos = segStart;
                segArray[segSize].segLen=i-segStart;
                segSize++;

            }
            //记录分隔符本身
            segArray[segSize].tag = mbuf[i];
            segArray[segSize].segLen=1;
            segArray[segSize].startPos = i;
            segStart =i+1;
            segSize++;
        }
	}
	return failLine;
}


/*
主处理函数入口
./Input A.log -> A.zip

*/

// // ./THULR -I /apsarapangu/disk9/LogSeg/Android/0.log -O /apsarapangu/disk9/PillBox_test/Android.zip
// int main(int argc, char *argv[]){
// //TODO:
// //1. Fix parser bugs
// //2. Integrate compression methods
// //3. Batch small files

// 	// clock_t start = clock();
// 	int o;
// 	const char *optstring = "HhI:O:";
// 	srand(4);
// 	//Input Content
// 	string input_path; string output_path;
//     //Input A.log -> A.zip
// 	while ((o = getopt(argc, argv, optstring)) != -1)
// 	{
// 		switch (o)
// 		{
// 		case 'I':
// 			input_path = optarg;
// 			//strcpy(input,optarg);
// 			printf("sample file path: %s\n", input_path.c_str()); //clove++ 20200919: path
// 			break;
// 		case 'O':
// 			output_path = optarg;
// 			printf("template output path : %s\n", output_path.c_str());
// 			break;
//         case 'h':
// 		case 'H':
// 			printf("-I sample path\n");	//clove## 20200919:
// 			printf("-O template output path\n");
//             return 0;
// 			break;
// 		case '?':
// 			printf("error:wrong opt!\n");
// 			printf("error optopt: %c\n", optopt);
// 			printf("error opterr: %c\n", opterr);
// 			return 1;
// 		}
// 	}
	
// 	//Basic input check
// 	if (input_path == ""){
// 		printf("error : No input file\n");
// 		return -1;
// 	}
	
// 	if (output_path == ""){
// 		printf("error : No output\n");
// 		return -1;
// 	}

    
//     timeval stime_s = ___StatTime_Start();    
	    
//     char* mbuf = NULL;
//     int len = LoadFileToMem(input_path.c_str(), &mbuf); //Load sample file
//     if (len <= 0){
//         SysWarning("Read sample file failed!\n");
//         return -1;
//     }

//     SegTag segArray[MAXTOCKEN];
//     int segSize = 0;
//     int segStart = 0, lineStart = 0;

//     int nowLine = 0, nowSample = 0;
//     bool sampled = false;
//     LengthParser* parser = new LengthParser();

//     for (int i = 0; i < len; i++){
//         if(segSize == MAXTOCKEN - 1){
//             SysWarning("[WARNING] segSize out of bound\n");
//             nowLine++;
//             segSize = 0;
//             segStart = i + 1;
//             lineStart = i + 1;
//         }
//         if(mbuf[i] == '\n'){
//             if(i > segStart){
//                 segArray[segSize].tag = 0;
//                 segArray[segSize].startPos = segStart;
//                 segArray[segSize].segLen = i - segStart;
//                 segSize++;
//             }
//             parser -> parseTemplate(mbuf, segArray, segSize);
//             nowLine++;
//             segSize = 0;
//             segStart = i + 1;
//             lineStart = i + 1;
//             // if(i - lineStart > MAX_VALUE_LEN){
//             //     SysWarning("[WARNING] line length out of bound: %d\n", i - lineStart);
//             // }
//         }else if(strchr(delim, mbuf[i])){
//             if (i > segStart){
//                 segArray[segSize].tag = 0;
//                 segArray[segSize].startPos = segStart;
//                 segArray[segSize].segLen = i - segStart;
//                 segSize++;
//             }
//             segArray[segSize].tag = mbuf[i];
//             segArray[segSize].segLen = 1;
//             segArray[segSize].startPos = i;
//             segStart = i + 1;
//             segSize++;
//         }
//     }
    
//     //build varaible_mapping;
//     map<int, VarArray*> variable_mapping;  
//     for(auto &pool:parser -> LengthTemplatePool){
//         vector<templateNode*>* nowPool = pool.second;
//         for(auto &temp: *nowPool){
//             for(int i = 0; i < temp->varLength; i++){
//                 int nowTag = ((temp->Eid)<<POS_TEMPLATE) | (i<<POS_VAR);
//                 variable_mapping[nowTag] = new VarArray(nowTag, parser -> STC[temp->Eid]);
//             }
//         }
//     }

    
//     parser -> TemplatePrint();
// 	//Streaming processing(match, match again, extract varibales and output)
// 	double stime = ___StatTime_End(stime_s);

//     timeval mtime_s = ___StatTime_Start();
	
//     cout << "Start matching" << endl;
//     int nowline = 0;
//     int failLine = matchFile(mbuf, len, parser, variable_mapping, nowline);
    
//     double mtime = ___StatTime_End(mtime_s);

//     char* longStr = NULL;
//     int ll = parser -> getTemplate(&longStr);
//     FILE* template_file = fopen((output_path + ".templates").c_str(), "w");
//     fprintf(template_file, "%s", longStr);
//     if(longStr) delete longStr;
//     fclose(template_file);
    
//     FILE* variable_file = fopen((output_path + ".variables").c_str(), "w");
//     if(variable_file == NULL){
//         printf("Open output file failed\n");
//         return -1;
//     }
//     int SUBCOUNT = 0;
    
//     int SCnt = 0;
//     int MCnt = 0;
//     int DCnt = 0;

//     timeval vtime_s = ___StatTime_Start();
//     cout << "Start runtime pattern tarining" << endl;
// 	for(map<int, VarArray*>::iterator it = variable_mapping.begin(); it != variable_mapping.end(); it++){
// 		bool debug = false;
//         VarArray * varVector = it -> second;
//         int varTag = it ->first;
//         string NOWPATTERN = to_string(varTag) + " ";
//         int nowTemplate = varTag >> POS_TEMPLATE;
//         int nowVariable = (varTag >> POS_VAR) & 0xff;
//         int totLen = varVector -> nowPos;
//         int nType = 0;
//         int nLen = -1;
//         // for(int idx = 0; idx < totLen; idx++){
//         //     int tmpStart = varVector ->startPos[idx];
//         //     int tmpLen = varVector ->len[idx];
//         //     nLen = max(nLen, tmpLen);
//         //     for(int s = 0; s < tmpLen; s++) nType |= getTypeC(*(mbuf + tmpStart + s));
//         // }
//         NOWPATTERN += "S <V,63,999999>";

//         NOWPATTERN += "\n";
        
//         fprintf(variable_file, "%s", NOWPATTERN.c_str());
// 	}
//     double vtime = ___StatTime_End(vtime_s);
    
//     for(auto &temp: variable_mapping){
//         delete temp.second;
//     }
//     fclose(variable_file);
//     //printf("stime: %lfs, mtime: %lfs, vtime:%lfs\n", stime, mtime, vtime, ctime);
// //*/
// } 

extern "C" int trainer_wrapper(const char * sample_ptr, const char * output_path_ptr) {
	
	//Basic input check
	//

    std::string sample(sample_ptr);
    std::string output_path(output_path_ptr);

    timeval stime_s = ___StatTime_Start();    
	
    int len = sample.size();
    char* mbuf =  new char[len];
    std::copy(sample.begin(), sample.end(), mbuf);

    SegTag segArray[MAXTOCKEN];
    int segSize = 0;
    int segStart = 0, lineStart = 0;

    int nowLine = 0, nowSample = 0;
    bool sampled = false;
    LengthParser* parser = new LengthParser();

    for (int i = 0; i < len; i++){
        if(segSize == MAXTOCKEN - 1){
            SysWarning("[WARNING] segSize out of bound\n");
            nowLine++;
            segSize = 0;
            segStart = i + 1;
            lineStart = i + 1;
        }
        if(mbuf[i] == '\n'){
            if(i > segStart){
                segArray[segSize].tag = 0;
                segArray[segSize].startPos = segStart;
                segArray[segSize].segLen = i - segStart;
                segSize++;
            }
            parser -> parseTemplate(mbuf, segArray, segSize);
            nowLine++;
            segSize = 0;
            segStart = i + 1;
            lineStart = i + 1;
            // if(i - lineStart > MAX_VALUE_LEN){
            //     SysWarning("[WARNING] line length out of bound: %d\n", i - lineStart);
            // }
        }else if(strchr(delim, mbuf[i])){
            if (i > segStart){
                segArray[segSize].tag = 0;
                segArray[segSize].startPos = segStart;
                segArray[segSize].segLen = i - segStart;
                segSize++;
            }
            segArray[segSize].tag = mbuf[i];
            segArray[segSize].segLen = 1;
            segArray[segSize].startPos = i;
            segStart = i + 1;
            segSize++;
        }
    }
    
    //build varaible_mapping;
    map<int, VarArray*> variable_mapping;  
    for(auto &pool:parser -> LengthTemplatePool){
        vector<templateNode*>* nowPool = pool.second;
        for(auto &temp: *nowPool){
            for(int i = 0; i < temp->varLength; i++){
                int nowTag = ((temp->Eid)<<POS_TEMPLATE) | (i<<POS_VAR);
                variable_mapping[nowTag] = new VarArray(nowTag, parser -> STC[temp->Eid]);
            }
        }
    }

    
    // parser -> TemplatePrint();
	//Streaming processing(match, match again, extract varibales and output)
	double stime = ___StatTime_End(stime_s);

    timeval mtime_s = ___StatTime_Start();
	
    cout << "Start matching" << endl;
    int nowline = 0;
    int failLine = matchFile(mbuf, len, parser, variable_mapping, nowline);
    
    double mtime = ___StatTime_End(mtime_s);

    char* longStr = NULL;
    int ll = parser -> getTemplate(&longStr);
    FILE* template_file = fopen((output_path + ".templates").c_str(), "w");
    fprintf(template_file, "%s", longStr);
    if(longStr) delete longStr;
    fclose(template_file);
    
    FILE* variable_file = fopen((output_path + ".variables").c_str(), "w");
    if(variable_file == NULL){
        printf("Open output file failed\n");
        return -1;
    }
    int SUBCOUNT = 0;
    
    int SCnt = 0;
    int MCnt = 0;
    int DCnt = 0;

    timeval vtime_s = ___StatTime_Start();
    cout << "Start runtime pattern tarining" << endl;
	for(map<int, VarArray*>::iterator it = variable_mapping.begin(); it != variable_mapping.end(); it++){
		bool debug = false;
        VarArray * varVector = it -> second;
        int varTag = it ->first;
        string NOWPATTERN = to_string(varTag) + " ";
        int nowTemplate = varTag >> POS_TEMPLATE;
        int nowVariable = (varTag >> POS_VAR) & 0xff;
        int totLen = varVector -> nowPos;
        int nType = 0;
        int nLen = -1;
        // for(int idx = 0; idx < totLen; idx++){
        //     int tmpStart = varVector ->startPos[idx];
        //     int tmpLen = varVector ->len[idx];
        //     nLen = max(nLen, tmpLen);
        //     for(int s = 0; s < tmpLen; s++) nType |= getTypeC(*(mbuf + tmpStart + s));
        // }
        NOWPATTERN += "S <V,63,999999>";

        NOWPATTERN += "\n";
        
        fprintf(variable_file, "%s", NOWPATTERN.c_str());
	}
    double vtime = ___StatTime_End(vtime_s);
    
    for(auto &temp: variable_mapping){
        delete temp.second;
    }
    fclose(variable_file);
    //printf("stime: %lfs, mtime: %lfs, vtime:%lfs\n", stime, mtime, vtime, ctime);
//*/
    return 0;
}
