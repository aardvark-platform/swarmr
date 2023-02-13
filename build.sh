#!/bin/bash

dotnet tool restore
dotnet paket restore
dotnet build src/swarmr.sln -c Release
dotnet publish -c Release -r linux-x64 -p:PublishSingleFile=true --self-contained false -o bin/swarmr src/swarmr