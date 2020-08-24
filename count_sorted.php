<?php
/* Identify differences in line counts */
$file_in = '/tmp/sorted.tsv';

$num_aggregated=1;

$fp_in = fopen($file_in, "r");
$csv_content = [];
while ($data = fgetcsv($fp_in, 0, '\t'))
{
	array_push($csv_content, $data[0]);
	$num_aggregated++;
}
echo( "Read ${num_aggregated} CSV rows.\n" );
fclose($fp_in);

$fp_in = fopen($file_in,"r");

$num_lines = 0;
$text_content = [];
while(! feof($fp_in))
{
	$line = fgets($fp_in);
	if ($line != "") {
		$data = str_getcsv($line, '\t', '\"');
		array_push($text_content, $data[0]);
		  $num_lines++;
	}
}
fclose($fp_in);
echo( "Read ${num_lines}.\n" );

$idx = 0;
$done=false;
$arr_length = min(count($csv_content), count($text_content));
while( $idx < $arr_length && !$done) {
	if ($csv_content[$idx] !=  $text_content[$idx]) {

		echo "CSV: " . $csv_content[$idx] . "\n";
		echo "Text: " . $text_content[$idx] . "\n";
		$done = true;
	}
	$idx++;
}
