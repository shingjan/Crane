#!/bin/bash
pkill -9 python3
rm ../dfs.log
rm ../mmp.log
rm -rf ../dfs/
mkdir ../dfs/
git pull
