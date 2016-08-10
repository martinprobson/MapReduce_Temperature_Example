#!/usr/bin/awk -f
# vim: set ts=4 sw=4 ai:
BEGIN {
		FS="\t";
		max_temp = 0;
      }

      {
	  	year = $1; temp = $2 + 0;
	  	if (prev_year == "") {
			prev_year = year;
			max_temp = temp;
		} else if (prev_year == year && temp > max_temp) {
				max_temp = temp
				} else if (prev_year != year) {
				           print prev_year, max_temp;
						   prev_year = year;
						   max_temp = temp
					   }
      }

END   {
		if (max_temp != "")
			print prev_year,max_temp
	  }
