#include <iostream>
#include <vector>
#include <string.h>
#include "wavelet_tree_disk.h"

int main(int argc, const char *argv[1]) {
    // let the user input a char as the first argument

    const char *P = argv[1];
    auto to_hit = construct_wavelet_tree(P, strlen(P));
    
    //print out all the non-empty elements of to_hit
    for (size_t i = 0; i < to_hit.size(); i++) {
        if (to_hit[i].size() != 0) {
            std::cout << i << ": ";
            for (size_t j = 0; j < to_hit[i].size(); j++) {
                std::cout << to_hit[i][j];
            }
            std::cout << std::endl;
        }
    }

    std::cout << wavelet_tree_rank(to_hit, argv[2][0] , atoi(argv[3])) << std::endl;
}