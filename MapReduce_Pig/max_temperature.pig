/* Max temperature example - Apache pig version */
DEFINE extract_fields `extract_fields.awk` SHIP ('extract_fields.awk');
raw = LOAD '../data/ncdc' USING TextLoader();
temps = STREAM raw THROUGH extract_fields AS (year:chararray,temperature:int,quality:int);
filtered_temps = FILTER temps BY temperature != 9999 AND quality IN (0,1,4,5,9);
grouped_temps = GROUP filtered_temps BY year;
max_temp = FOREACH grouped_temps GENERATE group,MAX(filtered_temps.temperature);
DUMP max_temp;
