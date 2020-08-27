<?php

use MediaWiki\MediaWikiServices;
use MediaWiki\Revision\RevisionLookup;

$stagingDir = '/srv/mediawiki-staging/php-1.36.0-wmf.5';
$path = set_include_path ( $stagingDir );
require_once $stagingDir . '/maintenance/Maintenance.php';

// Write all output to /tmp since the script runs as www-data
$pageFile = '/tmp/pages.tsv';
$sortedPagesFile = '/tmp/sorted_pages.tsv';
$watchlistFile = '/tmp/watchlist.tsv';
$sortedWatchlistFile = '/tmp/sorted_watchlist.tsv';
$aggregateFile = '/tmp/aggregated_watchlist.tsv';
$finalFile = '/tmp/output.tsv';

define("CHUNK_SIZE", 100000);

/**
 * Maintenance script to write the watchlist to disk and create watcher counts for each page on the list.
 *
 * @ingroup Maintenance
 */
class DumpWatchlist extends Maintenance {
	public function __construct() {
		parent::__construct();
		$this->addDescription( 'Retrieve all watchlist records and write to disk in a sorted format.' );
	}

	private function dump_pages($file_out) {
		$start_idx = 1;
		$finished = false;
		$num_processed = 0;

		$dbw = $this->getDB( DB_MASTER, 'dump' );
		$opts = [ 'ORDER_BY' => ['page_title', 'page_namespace'] ];

		$this->output( "Dumping page records" );
		while (!$finished)
		{
			$end_idx = $start_idx + CHUNK_SIZE;
			$this->output( "." );
			$conds = [ 'page_id >= ' . $start_idx, 'page_id < ' . $end_idx ];
			$result = $dbw->select( 'page',
				[ 'page_title', 'page_namespace' ],
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
					// Row format: title, ns
					fputcsv($fp_out, [$row->page_title, $row->page_namespace], "\t", "\"");
					$num_processed++;
				}
				fclose($fp_out);
				$start_idx += CHUNK_SIZE;
			}
		}
		$this->output( "\nProcessed ${num_processed} page records.\n" );
	}

	private function dump_watchlist($file_out) {
		$start_idx = 1;
		$finished = false;
		$num_processed = 0;

		$dbw = $this->getDB( DB_MASTER, 'dump' );
		$opts = [ 'GROUP BY' => [ 'wl_title', 'wl_namespace'], 'ORDER_BY' => ['wl_title', 'wl_namespace'] ];

		$this->output( "Dumping watchlist records" );
		while (!$finished)
		{
			$end_idx = $start_idx + CHUNK_SIZE;
			$this->output( "." );
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
					fputcsv($fp_out, [$row->wl_title, $row->wl_namespace, $row->wl_count], "\t", "\"");
					$num_processed++;
				}
				fclose($fp_out);
				$start_idx += CHUNK_SIZE;
			}
		}
		$this->output( "\nProcessed ${num_processed} watchlist records.\n" );
	}

	private function sort($file_in, $file_out) {
		$cmd = "sort -o ${file_out} ${file_in}";
		$this->output( "Running '${cmd}'...\n" );
		shell_exec($cmd);
	}

	private function merge($file1_in, $file2_in, $file_out) {
		$cmd = "awk -F'\t' 'NR==FNR { a[$1 $2]=1 ; } NR>FNR { if ( $1 $2 in a ) print $3,$1,$2 }' OFS='\t' ${file1_in} ${file2_in} > ${file_out}";
		$this->output( "Running '${cmd}'...\n" );
		shell_exec($cmd);
	}

	private function aggregate($file_in, $file_out) {
		$fp_in = fopen($file_in, "r");
		$fp_out = fopen($file_out, "w");
		$last_key = "";
		$last_row = null;

		$this->output( "Aggregating" );
		$num_aggregated=0;
		while (!feof($fp_in))
		{
			$line = fgets($fp_in);
			$data = str_getcsv($line, "\t");
			if ($line != "") {
				$num_aggregated++;
				$ns_title = $data[1] . "::" . $data[0];	// NS + title

				if ($ns_title != $last_key)
				{
					if ($last_row) {
						fputcsv($fp_out, $last_row, "\t");
					}
					$last_key = $ns_title;

					// Row format: title, ns, count
					$last_row = [ $data[0], $data[1], $data[2] ];
				}
				else
				{
					// Aggregate the counts
					$last_row[2] = $last_row[2] + $data[2];
				}
				if ($num_aggregated % CHUNK_SIZE == 0) {
					$this->output( "." );
				}
			}
		}
		if ($last_row) {
			$this->output( "\nAggregated ${num_aggregated} rows.\n" );
			fputcsv($fp_out, $last_row, "\t");
		}
		fclose($fp_in);
		fclose($fp_out);
	}

	public function execute() {
		global $watchlistFile, $sortedWatchlistFile, $aggregateFile, $pageFile, $sortedPagesFile, $finalFile;

		$starttime = microtime(true);

		unlink($watchlistFile);
		unlink($sortedWatchlistFile);
		unlink($aggregateFile);
		unlink($pageFile);

		$this->dump_pages($pageFile);
		$this->sort($pageFile, $sortedPagesFile);
		unlink($pageFile); // Get rid of original

		$this->dump_watchlist($watchlistFile);
		$this->sort($watchlistFile, $sortedWatchlistFile);
		unlink($watchlistFile); // Get rid of original

		$this->aggregate($sortedPagesFile, $aggregateFile);

		$this->merge($sortedWatchlistFile, $aggregateFile, $finalFile);
		// Get rid of sorted files
		unlink($sortedWatchlistFile);
		unlink($sortedPagesFile);
		unlink($aggregateFile);

		$endtime = microtime(true);
		$time_elapsed = round($endtime - $starttime, 2);

		$this->output( "Done! Finished in ${time_elapsed} seconds.\n" );
	}
}

$maintClass = DumpWatchlist::class;
require_once RUN_MAINTENANCE_IF_MAIN;
