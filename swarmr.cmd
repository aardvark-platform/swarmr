@echo off
SETLOCAL

PUSHD %~dp0\bin\swarmr
swarmr.exe %*
POPD