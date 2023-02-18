@echo off
SETLOCAL

pushd ..
dotnet tool restore
dotnet paket restore
dotnet publish -c Release -r win-x64 -p:PublishSingleFile=true --self-contained false -o test/bin/sum src/Sum
popd

mkdir data

.\bin\sum\Sum create  1000000 > .\data\data1.txt
.\bin\sum\Sum create  2000000 > .\data\data2.txt
.\bin\sum\Sum create 10000000 > .\data\data3.txt

pushd bin\sum
7z a -tzip ..\..\data\sum.zip *
popd

pushd data
7z a -tzip data1 data1.txt
7z a -tzip data2 data2.txt
7z a -tzip data3 data3.txt
popd 
