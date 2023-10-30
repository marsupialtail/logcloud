from setuptools import setup, Extension
import os

# Set the CXX environment variable to 'g++' before invoking setup
os.environ['CXX'] = 'g++'

ext_module = Extension(
    'rottnest.libindex',  # Change 'yourpackage' to your package name
    sources=['src/index.cc', 'src/vfr.cc', 'src/kauai.cc', 'src/plist.cc'],
    language = "c++",
    include_dirs=['aws-sdk-cpp/build/include/', 'libdivsufsort/build/include/', 'src'],
    library_dirs=['aws-sdk-cpp/build/lib/', 'aws-sdk-cpp/build/lib64/', 'libdivsufsort/build/lib/'],
    libraries=['divsufsort', 'aws-cpp-sdk-s3', 'aws-cpp-sdk-core', 'lz4', 'snappy', 'zstd'],
    extra_compile_args=['-O3', '-g', '-fPIC','-Wno-sign-compare', '-Wno-strict-prototypes', '-fopenmp', '-std=c++17'], 
    extra_link_args = ['-lgomp']
)

setup(
    name='rottnest',  # Change to your package name
    version='1.0',
    description='Description of your package',
    ext_modules=[ext_module],
    packages=['rottnest'],  # Change to your package name
    package_data={'rottnest': ['libindex.so']},
    entry_points={
        "console_scripts": [
            "search=rottnest.search:main",
            "index=rottnest.index:main"
        ],
    }, 
)
