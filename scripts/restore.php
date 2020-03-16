<?php
use Phidias\Db\Backup;

const VENDOR_DIR = "D:/repositories/composer/vendor/";
include VENDOR_DIR."autoload.php";

echo phpversion().": Phidias\Db\Backup\n";

if (!isset($argv[1]) || !isset($argv[2])) {
    echo "Usage: restore.php <user:pass@server/db> <filename>";
    exit;
}

$dsn = $argv[1];
$filename = $argv[2];

try {
    Backup::restore($dsn, $filename);
} catch (\Exception $e) {
    echo "ERROR: ".$e->getMessage()."\n";
}