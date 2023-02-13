@echo off
SETLOCAL
PUSHD %~dp0

dotnet tool restore
dotnet paket restore
REM dotnet build src/swarmr.sln -c Release
dotnet publish -c Release -r win-x64 -p:PublishSingleFile=true --self-contained -o bin/swarmr src/swarmr

POPD