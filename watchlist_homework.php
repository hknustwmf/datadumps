<?php

use MediaWiki\MediaWikiServices;
use MediaWiki\Revision\RevisionLookup;

$stagingDir = '/srv/mediawiki-staging/php-1.36.0-wmf.5';
$outFile = '/tmp/output.tsv';  // Write to tmp since it runs as php user
$sortedFile = '/tmp/sorted.tsv';  // Write to tmp since it runs as php user
$aggregateFile = '/tmp/aggregate.tsv';  // Write to tmp since it runs as php user
$pageCacheFile = '/tmp/pages.tree'; // Write to tmp since it runs as php user

$path = set_include_path ( $stagingDir );

require_once $stagingDir . '/maintenance/Maintenance.php';
require_once dirname(__FILE__) . '/btree.php';


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

	private function dump_pages($btree) {
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
				[ 'page_title', 'page_namespace', 'page_is_redirect' ],
				$conds,
				__METHOD__,
				$opts
			);

			$finished = ($result->numRows() == 0);

			if (!$finished)
			{
				foreach ( $result as $row )
				{
					$key = $row->page_namespace . "::" . $row->page_title;

					$btree->set($key, $row->page_is_redirect);
					$num_processed++;
				}
				$start_idx += CHUNK_SIZE;
			}
		}
		$this->output( "\nProcessed $num_processed page records.\n" );
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
		$this->output( "\nProcessed $num_processed watchlist records.\n" );
	}

	private function sort($file_in, $file_out) {
		$cmd = "sort -o ${file_out} ${file_in}";
		// $this->output( "Running '${cmd}'...\n" );
		$this->output( "Sorting...\n" );
		shell_exec($cmd);
	}

	private function aggregate($file_in, $file_out, $btree) {
		$fp_in = fopen($file_in, "r");
		$fp_out = fopen($file_out, "w");
		$last_key = "";
		$last_row = null;

		$this->output( "Aggregating" );
		$num_aggregated=0;
		$num_non_existent=0;
		while (!feof($fp_in))
		{
			$line = fgets($fp_in);
			$data = str_getcsv($line, "\t");
			if ($line != "") {
				$num_aggregated++;
				$ns_title = $data[1] . "::" . $data[0];	// NS + title
				if ($btree->get($ns_title) == null) {
					$num_non_existent++;
				}

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
					// Aggregate the counts
					$last_row[0] = $last_row[0] + $data[2];
				}
				if ($num_aggregated % CHUNK_SIZE == 0) {
					$this->output( "." );
				}
			}
		}
		if ($last_row) {
			if ($num_aggregated > 0) {
				$perc_bad = round(floatval($num_non_existent) * 100.0/floatval($num_aggregated), 2);
			}
			else {
				$perc_bad = 0.0;
			}
			$this->output( "\nAggregated ${num_aggregated} rows. ${num_non_existent} (${perc_bad}%) non-existing titles found.\n" );
			fputcsv($fp_out, $last_row, "\t");
		}
		fclose($fp_in);
		fclose($fp_out);
	}

	public function execute() {
		global $outFile, $sortedFile, $aggregateFile, $pageCacheFile;

		$starttime = microtime(true);

		unlink($outFile);
		unlink($sortedFile);
		unlink($aggregateFile);
		unlink($pageCacheFile);

		// open B+Tree; file does not have to exist
		$btree = btree::open($pageCacheFile);
		// btree::open() returns false if anything goes wrong
		if ($btree === FALSE) die('cannot open');

		$this->dump_pages($btree);

		$this->dump_watchlist($outFile);

		$this->sort($outFile, $sortedFile);

		// Get rid of original
		unlink($outFile);

		$this->aggregate($sortedFile, $aggregateFile, $btree);

		// Get rid of sorted as well as page cache
		unlink($sortedFile);
		unlink($pageCacheFile);

		$endtime = microtime(true);
		$time_elapsed = round($endtime - $starttime, 2);
		$btree = null; // Trigger destructor to close file.

		$this->output( "Done! Finished in ${time_elapsed} seconds.\n" );
	}
}

$maintClass = DumpWatchlist::class;
require_once RUN_MAINTENANCE_IF_MAIN;
