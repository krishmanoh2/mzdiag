#! /bin/bash

# version 0.01
# Author - KM
# Gather diagnostic output from materialized
# Inputs are Diagnostic query, time to sleep in-between runs, number of iterations, hostname to connect to, directory to write the output to (Filename will be hostname_query.out)
# Assumes psql is installed

usage () { echo "Usage : $0 -q <query_file> -t <sleep time between query runs> -h <mz host>  -D <Directory to write output to> -I <number of iterations>"; }

while getopts "q:t:h:D:I:" opts
do
case "${opts}" in
   q)
        q=${OPTARG}
        query=$q;;
   t)
        t=${OPTARG}
        sleept=$t;;
   h)
        h=${OPTARG}
        host=$h;;
   D)
        D=${OPTARG}
        directory=$D;;
   I)
        I=${OPTARG}
        iterations=$I;;
esac
done


if [ ! "$query" ] || [ ! "$sleept" ] || [ ! "$host" ] || [ ! "$directory" ] || [ ! "$iterations" ]
then
    usage
    exit 1
fi

DAY=`date +%F--%H-%M-%S`


#
FILESAFE=`basename $query`

FILE_OUT=""$directory"/diag_"$FILESAFE"_"$host"_"$DAY".out"

echo "Starting at $DAY" > $FILE_OUT
echo " " >> $FILE_OUT
echo " " >> $FILE_OUT
echo " " >> $FILE_OUT


for i in $( seq 1 $iterations )
do
echo "Iteration $i  " `date +%F--%H-%M-%S` >> $FILE_OUT
echo " " >> $FILE_OUT
psql  --field-separator '|' --pset footer -h $host -p 6875 materialize  -f $query >> $FILE_OUT
echo " " >> $FILE_OUT
sleep $sleept
done


