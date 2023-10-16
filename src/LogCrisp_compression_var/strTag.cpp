#include<cstdlib>
#include<cstdio>
#include<cstring>
#include<iostream>
#define symbol_TY 32
using namespace std;
const int CharTable[128] = {32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,1,1,1,1,1,1,1,1,1,1,32,32,32,32,32,32,32,2,2,2,2,2,2,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,32,32,32,32,32,32,4,4,4,4,4,4,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,32,32,32,32,32};

int getTypeC(int c){
    if(c < 0 || c >= 128) return symbol_TY;
    return CharTable[c];
}

int main(int argc, char** argv){
    int len = strlen(argv[1]);
    int Type = 0;
    for(int i = 0; i < len; i++){
        Type |= getTypeC(argv[1][i]);
    }
    printf("Now Type: %d\n", Type);
    return 0;
}