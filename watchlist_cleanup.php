<?php

use MediaWiki\MediaWikiServices;
use MediaWiki\Revision\RevisionLookup;

$stagingDir = '/srv/mediawiki-staging/php-1.36.0-wmf.5';
$outFile = '/tmp/output.tsv';  // Write to tmp since it runs as php user
$sortedFile = '/tmp/sorted.tsv';  // Write to tmp since it runs as php user
$aggregateFile = '/tmp/aggregate.tsv';  // Write to tmp since it runs as php user

$path = set_include_path ( $stagingDir );

require_once $stagingDir . '/maintenance/Maintenance.php';


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
		global $outFile, $sortedFile, $aggregateFile;

		unlink($outFile);
		unlink($sortedFile);
		unlink($aggregateFile);

		$this->output( "Done!\n" );
	}
}

$maintClass = CleanupWatchlist::class;
require_once RUN_MAINTENANCE_IF_MAIN;

