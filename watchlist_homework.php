<?php

use MediaWiki\MediaWikiServices;
use MediaWiki\Revision\RevisionLookup;

$stagingDir = '/srv/mediawiki-staging/php-1.36.0-wmf.5';
$outFile = '/tmp/output.tsv';  // Write to tmp since it runs as php user
$sortedFile = '/tmp/sorted.tsv';  // Write to tmp since it runs as php user
$aggregateFile = '/tmp/aggregate.tsv';  // Write to tmp since it runs as php user

$path = set_include_path ( $stagingDir );

require_once $stagingDir . '/maintenance/Maintenance.php';

define("CHUNK_SIZE", 100000);

/**
 * Maintenance script to write the watch list to disk
 *
 * @ingroup Maintenance
 */
class DumpWatchlist extends Maintenance {
	public function __construct() {
		parent::__construct();
		$this->addDescription( 'Retrieve all watchlist records and write to disk in a sorted format.' );
	}

	private function dump($file_out) {
		$start_idx = 1;
		$finished = false;
		$num_processed = 0;

		$dbw = $this->getDB( DB_MASTER, 'dump' );
		$opts = [ 'GROUP BY' => [ 'wl_title', 'wl_namespace'], 'ORDER_BY' => ['wl_title', 'wl_namespace'] ];

		while (!$finished)
		{
			$end_idx = $start_idx + CHUNK_SIZE;
			$this->output( "Dumping records ${start_idx} to ${end_idx}...\n" );
			$conds = [ 'wl_id >= ' . $start_idx, 'wl_id < ' . $end_idx ];
			$result = $dbw->select( 'watchlist',
				[ 'wl_title', 'wl_namespace', 'wl_count' => 'COUNT(wl_id)' ],
				$conds,
				__METHOD__,
				$opts
			);

			$finished = ($result->numRows() == 0);

			if (!$finished)
			{
				$fp_out = fopen($file_out, 'a');
				foreach ( $result as $row )
				{
					// Row format: title, ns, count
					fputcsv($fp_out, [$row->wl_title, $row->wl_namespace, $row->wl_count], "\t");
					$num_processed++;
				}
				fclose($fp_out);
				$start_idx += CHUNK_SIZE;
			}
		}
		$this->output( "Processed $num_processed records.\n" );
	}

	private function sort($file_in, $file_out) {
		$cmd = "sort -o ${file_out} ${file_in}";
		$this->output( "Running '${cmd}'...\n" );
		shell_exec($cmd);
	}

	private function aggregate($file_in, $file_out) {
		$fp_in = fopen($file_in, "r");
		$fp_out = fopen($file_out, "w");
		$last_key = "";
		$last_row = null;

		$this->output( "Aggregating...\n" );
		$n=1;
		while ($data = fgetcsv($fp_in, 0, "\t"))
		{
			$ns_title = $data[1] . "::" . $data[0];
			if ($ns_title != $last_key)
			{
				if ($last_row) {
					fputcsv($fp_out, $last_row, "\t");
				}
				$last_key = $ns_title;

				// Row format: count, title, ns
				$last_row = [ $data[2], $data[0], $data[1] ];
			}
			else
			{
				// Agregate the counts
				$last_row[0] = $last_row[0] + $data[2];
			}
			$n++;
			if ($n % CHUNK_SIZE == 0) {
				$this->output( "Aggregated ${n} rows...\n" );
			}
		}
		if ($last_row) {
			$this->output( "Aggregated ${n} rows...\n" );
			fputcsv($fp_out, $last_row, "\t");
		}
		fclose($fp_in);
		fclose($fp_out);


	}

	public function execute() {
		global $outFile, $sortedFile, $aggregateFile;

		$starttime = microtime(true);

		unlink($outFile);
		unlink($sortedFile);
		unlink($aggregateFile);

		$this->dump($outFile);

		$this->sort($outFile, $sortedFile);

		// Get rid of original
		unlink($outFile);

		$this->aggregate($sortedFile, $aggregateFile);

		// Get rid of sorted
		unlink($sortedFile);

		$endtime = microtime(true);
		$time_elapsed = round($endtime - $starttime, 2);

		$this->output( "Done! Finished in ${time_elapsed} seconds.\n" );
	}
}

$maintClass = DumpWatchlist::class;
require_once RUN_MAINTENANCE_IF_MAIN;
