#!/bin/bash  
rm *.txt  
for i in 1 2 4 8 16 32 64 ;  
do  
echo $1-$i-calc.txt
./calc.out $1 $i > $1-$i-calc.txt
echo $1-$i-hg.txt
./hg.out $1 $i > $1-$i-hg.txt
echo $1-$i-pb.txt
./pb.out $1 $i > $1-$i-pb.txt  
done
