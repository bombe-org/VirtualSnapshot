rm *.txt
echo =====  CALC  =====
./calc.out $1 1 > $1-1-calc.txt
./calc.out $1 2 > $1-2-calc.txt
./calc.out $1 4 > $1-4-calc.txt
./calc.out $1 8 > $1-8-calc.txt
./calc.out $1 16 > $1-16-calc.txt
./calc.out $1 32 > $1-32-calc.txt
./calc.out $1 64 > $1-64-calc.txt
./calc.out $1 128 > $1-128-calc.txt
./calc.out $1 256 > $1-256-calc.txt
./calc.out $1 512 > $1-512-calc.txt
./calc.out $1 1024 > $1-1024-calc.txt
echo =====  HG  =====
./hg.out $1 1 > $1-1-hg.txt
./hg.out $1 2 > $1-2-hg.txt
./hg.out $1 4 > $1-4-hg.txt
./hg.out $1 8 > $1-8-hg.txt
./hg.out $1 16 > $1-16-hg.txt
./hg.out $1 32 > $1-32-hg.txt
./hg.out $1 64 > $1-64-hg.txt
./hg.out $1 128 > $1-128-hg.txt
./hg.out $1 256 > $1-256-hg.txt
./hg.out $1 512 > $1-512-hg.txt
./hg.out $1 1024 > $1-1024-hg.txt
echo =====  PB  =====
./pb.out $1 1 > $1-1-pb.txt
./pb.out $1 2 > $1-2-pb.txt
./pb.out $1 4 > $1-4-pb.txt
./pb.out $1 8 > $1-8-pb.txt
./pb.out $1 16 > $1-16-pb.txt
./pb.out $1 32 > $1-32-pb.txt
./pb.out $1 64 > $1-64-pb.txt
./pb.out $1 128 > $1-128-pb.txt
./pb.out $1 256 > $1-256-pb.txt
./pb.out $1 512 > $1-512-pb.txt
./pb.out $1 1024 > $1-1024-pb.txt
