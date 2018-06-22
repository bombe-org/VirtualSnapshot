#!/bin/bash  
rm *.txt 
echo size , thread_cnt, timestamp, tpms
#for s in 250000 500000 1000000 2000000
for s in 25000
do
for i in 1 2 4 8 16 32 64 ;  
do  
./calc.out $s $i 
./hg.out $s $i
./pb.out $s $i  
done
done

