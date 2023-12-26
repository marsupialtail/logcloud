#!/bin/bash
set -e -u -x

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
	# libcurl is different between centos and ubuntu
    	auditwheel repair "$wheel" --no-update-tags --plat manylinux_2_34_x86_64 --exclude libcurl.so.4 -w ./wheelhouse/
    fi
}

dnf install epel-release -y
dnf update -y
yum install -y epel-release || sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1).noarch.rpm
yum install -y https://apache.jfrog.io/artifactory/arrow/centos/$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)/apache-arrow-release-latest.rpm
yum install -y --enablerepo=epel arrow-devel # For C++
yum install -y --enablerepo=epel arrow-glib-devel # For GLib (C)
yum install -y --enablerepo=epel arrow-dataset-devel # For Apache Arrow Dataset C++
yum install -y --enablerepo=epel arrow-dataset-glib-devel # For Apache Arrow Dataset GLib (C)
yum install -y --enablerepo=epel arrow-acero-devel # For Apache Arrow Acero
yum install -y --enablerepo=epel parquet-devel # For Apache Parquet C++
yum install -y --enablerepo=epel parquet-glib-devel # For Apache Parquet GLib (C)
# Install dependencies
yum install -y glog-devel make cmake gcc-c++ unzip zlib-devel openssl-devel libzstd-devel snappy-devel lz4-devel libcurl-devel

git clone https://github.com/aws/aws-sdk-cpp.git
cd aws-sdk-cpp/
git submodule update --init --recursive
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE="Release" -DCMAKE_PREFIX_PATH=/usr/local/ -DCMAKE_INSTALL_PREFIX=/usr/local/ -DBUILD_ONLY='s3' -DENABLE_TESTING=OFF ..
make -j8
make install
cd ..
cd ..

git clone https://github.com/y-256/libdivsufsort.git
cd libdivsufsort
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE="Release" -DCMAKE_PREFIX_PATH=/usr/local/ -DCMAKE_INSTALL_PREFIX=/usr/local/ ..
make -j8
make install
cd ..
cd ..

PYTHONS="cp38-cp38 cp39-cp39 cp310-cp310 cp311-cp311"

mkdir wheelhouse

# Compile wheels
for PYTHON in ${PYTHONS}; do
    PYBIN="/opt/python/${PYTHON}/bin"
    "${PYBIN}/pip" wheel . --no-deps -w ./wheelhouse/
done

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/__w/rottnest/rottnest/aws-sdk-cpp/build/lib:/__w/rottnest/rottnest/aws-sdk-cpp/build/lib64:/__w/rottnest/rottnest/libdivsufsort/build/lib

# Bundle external shared libraries into the wheels
for whl in ./wheelhouse/*.whl; do
    repair_wheel "$whl"
done

for file in ./wheelhouse/*.whl; do
    py_version=$(echo "$file" | grep -oP 'cp3\d+')
    new_file=$(echo "$file" | sed "s/linux/manylinux_2_28/")
    mv "$file" "$new_file"
done

# Install packages and test
# for PYTHON in ${PYTHONS}; do
#     PYBIN="/opt/python/${PYTHON}/bin"
#     "${PYBIN}/pip" install pytest pyarrow
#     "${PYBIN}/pip" install pyquiver --no-index -f ./wheelhouse
#     "${PYBIN}/python" -mpytest ./src/python/tests
# done
