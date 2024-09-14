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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <map>
#include <cmath>
#include <vector>
#include <zstd.h>
#include "../constant.h"
#include "../Coffer.h"
using namespace std;


int main(int argc, char *argv[]){
//TODO:
//1. Fix parser bugs
//2. Integrate compression methods
//3. Batch small files

	// clock_t start = clock();
	int o;
	const char *optstring = "HhI:O:T:C:";
	srand(4);
	//Input Content
	string input_path; string output_path; string type; string compression_method;
//Input A.log -> A.zip
	while ((o = getopt(argc, argv, optstring)) != -1){ //input_path -> .zip file, output_path -> dir
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
        case 'C':
            compression_method = optarg;
            printf("compression method : %s\n", compression_method.c_str());
            break;
        case 'h':
		case 'H':
			printf("-I input path\n");	//clove## 20200919:
			printf("-O output path\n");
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
		printf("error : No input file\n");
		return -1;
	}
	
	if (output_path == ""){
		printf("error : No output\n");
		return -1;
	}
    if (compression_method == ""){
		compression_method = "Zstd";
	}
    struct stat buffer;
    if(stat(output_path.c_str(), &buffer) != 0){
        printf("Output directory does not exits\n");
        return -1;
        // if(!mkdir(output_path.c_str(), 777)){
        //     printf("Make directory failed\n");
        //     return -1;
        // }
    }

    FILE* zipFile = fopen(input_path.c_str(), "rb");
    if(zipFile == NULL){
        printf("uncompression read file error!\n");
        return -1;
    }
    
    //Uncompression meta file
    size_t destLen;
    size_t srcLen;
    fread(&destLen, sizeof(size_t), 1, zipFile);
    fread(&srcLen, sizeof(size_t), 1, zipFile);
    cout << "DestLen: " << destLen << " SrcLen: " << srcLen << endl;
    unsigned char* pLzma = new unsigned char[destLen + 5];
    fread(pLzma, sizeof(char), destLen, zipFile);
    unsigned char* meta;

    size_t decom_buf_size = ZSTD_getFrameContentSize(pLzma, destLen);
    meta = new Byte[decom_buf_size];
    size_t res = ZSTD_decompress(meta, decom_buf_size, pLzma, destLen);
    if(res != srcLen){
        printf("Meta uncompress failed with %lu \n", res);
        return -1;
    }
    delete [] pLzma;

    int fstart = 0;
    fstart += sizeof(size_t) + sizeof(size_t) + destLen; //real file start
    
    map<int, Coffer*> coffer_map;
    coffer_map.clear();
    Coffer::readCoffer(meta, srcLen, coffer_map);
    delete [] meta;
    for(map<int, Coffer*>::iterator iit = coffer_map.begin(); iit != coffer_map.end(); iit++){
       //cout << "Now decompress: " << iit -> first << endl;
       Coffer* nowCoffer = iit -> second;
       int res = nowCoffer -> readFile(zipFile, fstart, compression_method);
       if(res < 0){
           cout << "read file failed" << endl;
           continue;
       }
       res = nowCoffer -> decompress(compression_method);
       //char * query = nowCoferr -> data;
       if(res < 0){
           cout << "decompress failed" << endl;
           continue;
       }
       nowCoffer -> printFile(output_path);
       //cout << "decompress: " << iit -> first << " successed" << endl; 
    }
    //cout << string(meta) << endl;
    
    //size_t outputLen = 0;
    //size_t inputLen = 0;
    //cout << s << endl;
    
}
