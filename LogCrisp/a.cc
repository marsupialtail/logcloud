#include <filesystem>
#include <fstream>

int main()
{
	std::ofstream log_file("compressed/0/log");
	log_file << "test" << "\n";
	log_file.close();
}

