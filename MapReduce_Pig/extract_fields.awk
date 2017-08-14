#!/usr/bin/awk -f
# vim: set ts=4 sw=4 ai:
#
#############################################################################################
## Create a subset of the ncdc temp data that just contains a tab-separated list of : -
##	year	temperature	quality
#############################################################################################
{ 
	year         = substr($0,16,4);
	temperature  = substr($0,88,5) + 0 ;
    quality      = substr($0, 93, 1);
	printf "%s\t%s\t%s\n",year,temperature,quality; 
}
