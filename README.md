# Omniture

This repository contains a super-cool PHP wrapper script (omniture_ingest_redshift_udf.php) that :

# opens the tar file in the OMNITURE directory and gets the hearder file and stores it in an comma delimited string
# and then executes a REDSHIFT query embedded in a bash script (omniture_ingest_redshift_udf.sh) that reads OMNITURE data into a single record per line
# and then uses a REDSHIFT UDF (omni_parser)and the comma delimited string so that the index fucntion can be used to find the varible locations.
   Basically uses the split_part function in Redshift to parse the tab delimited file and the UDF to find the location of the variable of interest.
   For example, this line get the username field from the raw data line : split_part(line,'\t',omni_parser('page_url'))

RUNS one-brand-at-a-time.  Slow but a neat concept idea to create a smaller OMNITURE Redshift data set.
Get up on this for processing the data in EMR using PIG.   Much faster.

