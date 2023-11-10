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

ext_module_rex = Extension(
    'rottnest.librex',  # Change 'yourpackage' to your package name
    sources=['src/rex.cc'],
    language = "c++",
    libraries=['zstd'],
    extra_objects = [ 'src/Trainer.o', 'src/Compressor.o'], 
    extra_compile_args=[ '-O3', '-g', '-fPIC','-Wno-sign-compare', '-Wno-strict-prototypes', '-fopenmp', '-std=c++17'], 
    extra_link_args = ['-lgomp', '-l:libarrow.so', '-l:libparquet.so']
)

setup(
    name='rottnest',  # Change to your package name
    version='1.0',
    description='Description of your package',
    ext_modules=[ext_module, ext_module_rex],
    packages=['rottnest'],  # Change to your package name
    package_data={'rottnest': ['libindex.so', 'librex.so']},
    install_requires=[
            'getdaft>=0.1.20',
            'pyarrow>=7.0.0',
            'duckdb',
            'boto3',
            'pandas',
            'polars>=0.18.0', # latest version of Polars generally
            'sqlglot', # you will be needing sqlglot. not now but eventually
            'tqdm',
            ], # add any additional packages that 
    entry_points={
        "console_scripts": [
            "rottnest-search=rottnest.search:main",
            "rottnest-index=rottnest.index:main"
        ],
    }, 
)
