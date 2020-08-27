<?php

use MediaWiki\MediaWikiServices;
use MediaWiki\Revision\RevisionLookup;

$stagingDir = '/srv/mediawiki-staging/php-1.36.0-wmf.5';
$path = set_include_path ( $stagingDir );
require_once $stagingDir . '/maintenance/Maintenance.php';

$pageFile = '/tmp/pages.tsv';
$sortedPagesFile = '/tmp/sorted_pages.tsv';
$watchlistFile = '/tmp/watchlist.tsv';
$sortedWatchlistFile = '/tmp/sorted_watchlist.tsv';
$aggregateFile = '/tmp/aggregated_watchlist.tsv';
$finalFile = '/tmp/output.tsv';

/**
 * Maintenance script to write the watch list to disk
 *
 * @ingroup Maintenance
 */
class CleanupWatchlist extends Maintenance {
	public function __construct() {
		parent::__construct();
		$this->addDescription( 'Retrieve all watchlist records and write to disk in a sorted format.' );
	}

	public function execute() {
		global $pageFile, $sortedPagesFile, $watchlistFile, $sortedWatchlistFile, $aggregateFile;

		unlink($pageFile);
		unlink($sortedPagesFile);
		unlink($watchlistFile);
		unlink($sortedWatchlistFile);
		unlink($aggregateFile);
		unlink($finalFile);

		$this->output( "Done!\n" );
	}
}

$maintClass = CleanupWatchlist::class;
require_once RUN_MAINTENANCE_IF_MAIN;

