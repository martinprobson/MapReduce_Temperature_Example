#!/usr/bin/env bash
# vim: set ts=4 sw=4 ai:
echo
for ncdc_file in ../data/ncdc/*
do
	fname=`basename $ncdc_file`
	year=${fname:13:4}
	max_temp=`awk '{ temp = substr($0,88,5) + 0 ;
	       q = substr($0, 93, 1);
		   if (temp !=9999 && q ~ /[01459]/ && temp > max) max = temp }
		 END { print max }' $ncdc_file`
	echo -e "$year\t$max_temp"
done 
