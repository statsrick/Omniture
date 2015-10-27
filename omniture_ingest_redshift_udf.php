<?php

//COMMAND FOR EXECUTION:  omniture_ingest_redshift_udf.php 2015/10/20

$dt=$argv[1];
$ddt=str_replace('/','-',$dt);
$brands=array('hmagcosmopolitan_',
'hmaggoodhousekeeping_',
'hmagelle_',
'hmagseventeen_',
'hmagesquire_',
'hmagcountryliving_',
'hmagharpersbazaar_',
'hmagpopularmechanics_',
'hmagwomansday_',
'hmagmarieclaire_',
'hmagroadandtrack_',
'hmaghousebeautiful_',
'hmagredbookmag_',
'hmagdelish_',
'hmagelledecor_',
'hmagveranda_',
'hmagtownandcountry_',
'hmagquickandsimple_',
'hmagdoctorozmag_',
'hmagbestproducts_',
'hcfcardriverprod_');

system("/bin/bash /home/rmcfarland/rmcfarland/test_sm_trunc.sh");
exec("/usr/bin/s3cmd ls --recursive s3://hearstlogfiles/adobe/omni-mgweb/$dt/*",$s3list);

foreach($brands as $b) {

 // GET HEADERS
 system("/usr/bin/s3cmd get --force s3://hearstlogfiles/adobe/omni-mgweb/".$dt."/".$b.$ddt."-lookup_data.tar.gz /home/rmcfarland/rmcfarland/lookup.tar.gz");
 $lit = scandir('phar:///home/rmcfarland/rmcfarland/lookup.tar.gz');
 $headers = file_get_contents('phar:///home/rmcfarland/rmcfarland/lookup.tar.gz/column_headers.tsv');
 $h="\'".trim(str_replace("\t","\',\${NL}\'",$headers))."\'";
 //print $h;

 // CREATE MANIFESTS
 $fout = fopen('/home/rmcfarland/rmcfarland/manifest', 'w');
 fwrite($fout,"{\n");
 fwrite($fout,"  \"entries\": [\n");
 foreach($s3list as $line) {
 $j=explode("  ",strrev($line));
 $size=substr($line,17,10);
 if ($size>0 and strrpos(strrev($j[0]),'tsv.gz')>0 and strrpos(strrev($j[0]),$b)>0) {
    fwrite($fout,"   {\"url\":\"".strrev($j[0])."\", \"mandatory\":true},\n");
    }
 } //s3foreach
 fwrite($fout,"   {\"url\":\"s3://hearstdataservices/redshiftmanifests/omni_dummy.tsv.gz\", \"mandatory\":true}\n");
 fwrite($fout,"  ]\n");
 fwrite($fout,"}\n");
 fclose($fout);

 system("/usr/bin/s3cmd put /home/rmcfarland/rmcfarland/manifest s3://hearstdataservices/redshiftmanifests/manifest");
 system("/bin/bash /home/rmcfarland/rmcfarland/test_sm.sh $b $dt $h");  
} //brandsforeach
?>


