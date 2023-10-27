#define symbol_TY 32
const int CharTable[128] = {32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,1,1,1,1,1,1,1,1,1,1,32,32,32,32,32,32,32,2,2,2,2,2,2,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,32,32,32,32,32,32,4,4,4,4,4,4,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,32,32,32,32,32};

inline int get_type(const char* query){
    int len = strlen(query);
    int Type = 0;
    for(int i = 0; i < len; i++){
        int c = (int)query[i];
        Type |= (c < 0 || c >= 128) ? symbol_TY : CharTable[c];
    }
    return Type;
}

// figure out all the possible types from 1 to 63 that contains all the bits that type has
inline std::vector<int> get_all_types(int type) {
    std::vector<int> types = {};
    for (int i = 1; i <= 63; ++i) {
        if ((type & i) == type) {
            types.push_back(i);
        }
    }
    return types;
}