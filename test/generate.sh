#!/bin/bash

pushd ..
dotnet tool restore
dotnet paket restore
dotnet publish -c Release -r linux-x64 -p:PublishSingleFile=true --self-contained false -o test/bin/sum src/Sum
popd

mkdir data -p

./bin/sum/Sum create  1000000 > ./data/data1.txt
./bin/sum/Sum create  2000000 > ./data/data2.txt
./bin/sum/Sum create 10000000 > ./data/data3.txt


pushd bin/sum
zip ../../data/sum.zip *
popd

pushd data
zip data1 data1.txt
zip data2 data2.txt
zip data3 data3.txt
popd 
