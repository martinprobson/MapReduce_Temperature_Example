#!/usr/bin/env bash
# vim: set ts=4 sw=4 ai:
#
#############################################################################################
## Create a subset of the ncdc temp data that just contains a tab-separated list of : -
##	year	temperature	quality
#############################################################################################
echo
for ncdc_file in ../data/ncdc/*
do
	fname=`basename $ncdc_file`
	year=${fname:13:4}
	awk '{ 
	    year = substr($0,16,4);
		temp = substr($0,88,5) + 0 ;
	    q = substr($0, 93, 1);
		printf "%s\t%s\t%s\n",year,temp,q; } ' $ncdc_file
done 
