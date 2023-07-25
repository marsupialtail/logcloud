curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
sudo apt install cmake
sudo apt install g++
sudo apt install unzip
sudo apt install zlib1g-dev
sudo apt install libssl-dev

git clone https://github.com/aws/aws-sdk-cpp.git
cd aws-sdk-cpp/
git submodule update --init --recursive
sudo apt-get install libcurl4-openssl-dev
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE="Release" -DCMAKE_INSTALL_PREFIX=. -DBUILD_ONLY='s3' -DENABLE_TESTING=OFF ..
make -j8
sudo make install
cd ..
cd ..

git clone https://github.com/y-256/libdivsufsort.git
cd libdivsufsort
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE="Release" -DCMAKE_INSTALL_PREFIX=. ..
make -j8
sudo make install
cd ..
cd ..
