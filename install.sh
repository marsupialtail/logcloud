sudo apt update
sudo apt install -y make cmake g++ unzip zlib1g-dev libssl-dev libzstd-dev libsnappy-dev liblz4-dev

# first install the aws cli. I don't use it but you probably need it later to sync the index up to s3

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# install the AWS C++ SDK

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

# install libdivsufsort from source

git clone https://github.com/y-256/libdivsufsort.git
cd libdivsufsort
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE="Release" -DCMAKE_INSTALL_PREFIX=. ..
make -j8
sudo make install
cd ..
cd ..

# pyarrow 13.0.0 will not work!
sudo apt install -y python3-pip
pip3 install pyarrow==12.0.0
export PYARROW_PATH=$(python3 -c "import pyarrow; print(pyarrow.__file__.replace('__init__.py',''))")
g++ -O3 -g -D_GLIBCXX_USE_CXX11_ABI=0 src/rex.cc src/Trainer.o src/Compressor.o -I ${PYARROW_PATH}/include -L ${PYARROW_PATH} -o rex -L. -l:libarrow.so.1200 -l:libparquet.so.1200 -l:libzstd.a

