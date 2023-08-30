#include "LengthParser.h"
#include "util.h"
#include<algorithm>
#include<vector>
#include<cstring>
#include<cstdio>
#include<map>
using namespace std;
LengthParser::LengthParser() {
    now_eid = 1;
    delim = " \t:=,";
    delimCount = 5;
    DELIM = (char**)malloc(sizeof(char*) * 128);
    for (int i = 0; i < 128; i ++){
        DELIM[i] = (char*)malloc(sizeof(char) * 2);
        DELIM[i][0] = (char)i;
        DELIM[i][1] = '\0';
    }
    LengthTemplatePool.clear();
}
LengthParser::~LengthParser(){
    for (int i = 0; i < 128; i++){
        if(DELIM[i]) free(DELIM[i]);
    }
    
    free(DELIM);
    for(map<int, std::vector<templateNode*>* >::iterator it = LengthTemplatePool.begin(); it != LengthTemplatePool.end(); it++){
        std::vector<templateNode*>* nowPool = it ->second;
        for(std::vector<templateNode*>::iterator iit = nowPool ->begin(); iit != nowPool ->end(); iit++){
            if(*iit) delete *iit;
        }
        if(it ->second) delete it->second;
    }
}

void LengthParser::STCTCOut(string output_path, double sampleRate){
    string longStr = "";
    for (auto temp: TC){
        longStr += "E" + to_string(temp.first) + " TC:" + to_string(temp.second) + " STC:" + to_string(STC[temp.first] / sampleRate) + "\n";
    }
    FILE* fw = fopen(output_path.c_str(), "w");
    fprintf(fw, "%s", longStr.c_str());
    fclose(fw);
}

int LengthParser::STCTC(double sampleRate){
    int max_time = 0;
    for (auto temp: TC){
        int now_key = temp.first;
        int tc_result = temp.second;
        int stc_result = (STC.find(now_key) == STC.end()) ? 0 : STC[now_key];

        int time = (tc_result) / (stc_result / sampleRate);
        max_time = (time > max_time) ? time : max_time;
    }
    return max_time;
}

int LengthParser::parseTemplate(char * logs, SegTag* segArray, int token_size){
    bool merged = false;
    int hitEid = -1;
    if (LengthTemplatePool.find(token_size) == LengthTemplatePool.end()){ //Without this length
        vector<templateNode*> * newLength = new vector<templateNode*>;
        newLength ->clear();
        LengthTemplatePool[token_size] = newLength;
        hitEid = now_eid;
        STC[hitEid] = 1;
        templateNode* tempnode = new templateNode(now_eid++, logs, segArray, token_size);
        newLength ->push_back(tempnode);
        //cout << "New Length Create Length: " << token_size << "||" << tempnode -> print() << endl;
        return hitEid;
    }
    
    vector<templateNode*> * nowPool = LengthTemplatePool[token_size];
    for(auto &temp:*nowPool){
        double similarity = temp ->parseMatch(logs, segArray, token_size);
        SysDebug("Similarity %f on template %d\n", similarity, temp ->Eid);
        if (similarity > 0.5){ //merge the template
            temp ->merge(logs, segArray, token_size);
            merged = true;
            hitEid = temp->Eid;
            STC[hitEid]++;
            break;
        }
    }    
    if (token_size != 0 && !merged){
        hitEid = now_eid;
        STC[hitEid] = 1;
        templateNode* tempnode = new templateNode(now_eid++, logs, segArray, token_size);
        nowPool -> push_back(tempnode);
        //cout << "Create Length: " << token_size << "||" << tempnode -> print() << endl;
    }
    return hitEid;
}


int LengthParser::SearchTemplate(char* logs, SegTag* segArray, int tocken_size, map<int, VarArray*>& varMapping, bool extract){
    //char nowVar[MAX_VALUE_LEN];
    if (LengthTemplatePool.find(tocken_size) == LengthTemplatePool.end()){ //Without this length match failed
        SysDebug("No length\n");
        return -1;
    }
    bool find = false; 
    vector<templateNode*>* nowPool = LengthTemplatePool[tocken_size];
    //printf("nowPool: %d\n", nowPool->size());//大约20-40左右的样子，不是很多
    for(auto &temp:*nowPool){
        if (temp ->matchMatch(logs, segArray, tocken_size, headLength) != -1)
        { //matched
            if (TC.find(temp ->Eid) != TC.end()){
                TC[temp->Eid]++;
            }else{
                TC.insert(pair<int, int>(temp->Eid, 1));
            }
            if (!extract){
                return temp ->Eid;
            }else{
                int totVar = temp -> varLength;
                int now_eid = temp -> Eid;
                //cout << "matched to " << now_eid << " totVar: " << temp->varLength << endl;
                for (int i = 0; i < totVar; i++){
                    int nowName = (now_eid<<POS_TEMPLATE) | (i<<POS_VAR);
                    //int nowName = (i)<<16 + now_eid;//小端存储思路，高16位存储var
                    varMapping[nowName] ->Add(segArray[temp ->varIndex[i]].startPos, segArray[temp ->varIndex[i]].segLen);
                    
                }
                return temp->Eid;
            }  
        }
    }
    return -1;
}

void LengthParser::TemplatePrint(){
    for(std::map<int, vector<templateNode*>* >::iterator it = LengthTemplatePool.begin(); it != LengthTemplatePool.end(); it++){
        cout << "Template with length: " << it ->first << endl;
        vector<templateNode*>* nowPool = it -> second;
        for(auto &temp:*nowPool){
            cout << temp ->print();
        }
        
    }
}

void LengthParser::counterReset(){
    for(auto &temp: TC){
        temp.second = 0;
    }
}

bool pairCmp(pair<int, int> t1, pair<int, int> t2){
    return (t1.second > t2.second);            
}

int LengthParser::getTemplate(char** longStr){
    string output = "";
    int count = 0;
    vector<pair<int, int> > container(TC.begin(), TC.end());
    sort(container.begin(), container.end(), pairCmp);

    map<int, string> E2S; //Eid to String
    map<int, int> E2T; //Eid to Length

    map<int, int> L2P; //Length to Pointer
    int * LC = new int[MAX_SEGSIZE]; //Length start
    int * LS = new int[MAX_SEGSIZE]; //Length start
    for(int i = 0; i < MAX_SEGSIZE; i++) LC[i] = -1;
    for(std::map<int, vector<templateNode*>* >::iterator it = LengthTemplatePool.begin(); it != LengthTemplatePool.end(); it++){
        vector<templateNode*>* nowPool = it -> second;
        LC[it ->first] = it ->second->size();
        for(auto &temp: *nowPool){
            if(TC[temp -> Eid] == 0) continue;
            E2S[temp->Eid] = temp -> output();
            E2T[temp->Eid] = it ->first;
        }
    }

    int nowPos = 0;
    int lengthCnt = 0;
    for(int i = 0; i < MAX_SEGSIZE; i++){
        if(LC[i] != -1){
            int nSize = LC[i];
            LS[i] = nowPos;
            nowPos += nSize;
            lengthCnt++;
            L2P[i] = 0;
        }
    }

    //output += to_string(lengthCnt) + " " + to_string(nowPos) + " ";
    for(int i = 0; i < MAX_SEGSIZE; i++){
        if(LC[i] != -1){
            output += "<" + to_string(i) + "," + to_string(LS[i]) + "," + to_string(LC[i]) + ">";
            output += " ";
        }
    }
    output += "\n";

    for(vector<pair<int, int> >::iterator it = container.begin(); it != container.end(); it++){
        int nLength = E2T[it ->first];
        if(nLength > MAX_SEGSIZE) continue;
        // int nPosition = LS[nLength] + L2P[nLength];
        // L2P[nLength]++;
        output += "E" + to_string(it -> first) + " " + to_string(TC[it ->first]) + " " + E2S[it -> first] + "\n";
        count++;
    }
    int ol = output.size();
    //char* longStr = *(_longStr);
    *longStr = new char[ol + 5];
    //cout << output << endl;
    memcpy(*longStr, output.c_str(), sizeof(char) * ol);
    //cout << "here " << longStr << endl;
    (*longStr)[ol] = '\0';
    return count;
}

void LengthParser::TemplateOutput(string input_path){
    FILE* fo = fopen(input_path.c_str(), "w");
    if (fo == NULL){
        printf("Tempalte Output failed\n");
        return;
    }
    for(std::map<int, vector<templateNode*>* >::iterator it = LengthTemplatePool.begin(); it != LengthTemplatePool.end(); it++){
        //cout << "Template with length: " << it ->first << endl;
        vector<templateNode*>* nowPool = it -> second;
        for(auto &temp:*nowPool){
            if (TC[temp ->Eid] == 0) continue;
            fprintf(fo, "E%d %d %s\n", temp ->Eid, TC[temp ->Eid], temp ->output().c_str());
        }
    }
    fclose(fo);
}
