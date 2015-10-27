#!/bin/bash

DBHOST=hearst-dw-mediaosdev.chekqimmelgb.us-west-2.redshift.amazonaws.com
DBPORT=5439
DBNAME=hdw
DBUSER=
DBPASS=
NL=$'\n'

# Secure temp files
export PGPASSFILE=`mktemp /home/rmcfarland/rmcfarland/logs/_sql_pass.XXXXXX`
cmds=`mktemp /home/rmcfarland/rmcfarland/logs/_sql_cmds.XXXXXX`
logs=`mktemp /home/rmcfarland/rmcfarland/logs/_sql_logs.XXXXXX`


cat >$PGPASSFILE << EOF
$DBHOST:$DBPORT:$DBNAME:$DBUSER:$DBPASS
EOF

: <<'****'

****



cat > $cmds << EOF
drop table if exists tmp_omniture_hmgweb1;
create table tmp_omniture_hmgweb1
(line varchar(max) ENCODE lzo);
copy tmp_omniture_hmgweb1 from 's3://hearstdataservices/redshiftmanifests/manifest'
CREDENTIALS 'aws_access_key_id=;aws_secret_access_key='
MANIFEST GZIP IGNOREBLANKLINES ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS REMOVEQUOTES maxerror 100000;

GRANT USAGE ON LANGUAGE plpythonu TO $DBUSER;
drop  function  omni_parser(obj varchar);
create or replace function  omni_parser(obj varchar)
RETURNS int
IMMUTABLE AS \$\$
fieldnames=($3)
return fieldnames.index(obj)+1
\$\$LANGUAGE plpythonu;

insert into tmp_omniture_hmgweb2 (
select
to_date('$2','YYYY/MM/DD') dt
,rtrim('$1','_') username
,split_part(line,'\t',omni_parser('page_url')) page_url
,split_part(line,'\t',omni_parser('hit_source')) hit_source
,split_part(line,'\t',omni_parser('post_page_event')) post_page_event
,split_part(line,'\t',omni_parser('visid_low')) visid_low
,split_part(line,'\t',omni_parser('visid_high')) visid_high
,split_part(line,'\t',omni_parser('visit_num')) visit_num
,split_part(line,'\t',omni_parser('post_visid_low')) post_visid_low
,split_part(line,'\t',omni_parser('post_visid_high')) post_visid_high
,split_part(line,'\t',omni_parser('hit_time_gmt')) hit_time_gmt
,split_part(line,'\t',omni_parser('visit_start_time_gmt')) visit_start_time_gmt
from tmp_omniture_hmgweb1
);

EOF
psql -d $DBNAME -h $DBHOST -p $DBPORT -U $DBUSER -f $cmds >$logs 2>&1




