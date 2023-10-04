#!/bin/bash
for(( i=0;i < 100;i++ ))     #定义for循环i变量初始值为0，循环条件i小于等于5，每循环一次i自加1
do
#for循环每循环一次执行一次test
# go test -run 2B

# go test -run  ^TestBasicAgree2B$  
# go test -run ^TestRPCBytes2B$ 
# go test -run ^TestLeaderFailure2B$
go test -count 10 -run  ^TestFollowerFailure2B$
# go test -run ^TestFailAgree2B$
# go test -run ^TestFailNoAgree2B$
# go test -run ^TestConcurrentStarts2B$
# go test -run ^TestRejoin2B$
# go test -run ^TestBackup2B$
# go test -run ^TestCount2B$

done                      #for语句结束标志             
