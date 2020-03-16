<?php
namespace Phidias\Db;

class Backup
{
    public static function create($connectionString, $filename)
    {
    }

    public static function restore($connectionString, $filename)
    {
        echo "Restoring $connectionString $filename\n";

        if (!class_exists('\ZipArchive')) {
            throw new \Exception("ZIP extension not installed ('ZipArchive' class not available)");
        }

        if (!is_file($filename)) {
            throw new \Exception("Invalid file '$filename'");
        }


        // Connect to DB
        $credentials = Db::toCredentials($connectionString);
        if (!$credentials) {
            throw new \Exception("Invalid connection string.  Must be: user:pass@server/database");
        }
        $db = Db::connect($credentials);

        // Extract ZIP contents to temp folder
        $tempdir = sys_get_temp_dir().DIRECTORY_SEPARATOR.'phidias_db_bak_'.time();
        mkdir($tempdir);

        echo "Extracting to '$tempdir'\n";
        $zip = new \ZipArchive;
        if ($zip->open($filename) === true) {
            $zip->extractTo($tempdir);
            $zip->close();
        } else {
            rmdir($tempdir);
            throw new \Exception("Could not extract file '$filename'");
        }

        // Query LOAD DATA for each .csv in the folder
        $handle = opendir($tempdir);
        if (!$handle) {
            throw new \Exception("Could not read extracted files in '$tempdir'");
        }

        $db->query("SET FOREIGN_KEY_CHECKS = 0");
        $db->query("SET @DISABLE_TRIGGERS=1");

        while (($entry = readdir($handle)) !== false) {
            if ($entry == "." || $entry == "..") {
                continue;
            }

            if (substr($entry, -4) == '.csv') {
                $table = substr($entry, 0, -4);
                $fullFile = $tempdir.DIRECTORY_SEPARATOR.$entry;
                $fullFile = str_replace(DIRECTORY_SEPARATOR, '/', $fullFile);


                echo $table.":\t";
                try {
                    $db->query("TRUNCATE $table");
                    $db->query("LOAD DATA LOCAL INFILE \"$fullFile\" INTO TABLE $table");
                    echo "OK\n";
                } catch (\Exception $e) {
                    echo $e->getMessage()."\n";
                }
            }

            unlink($tempdir.DIRECTORY_SEPARATOR.$entry);
        }

        $db->query("SET FOREIGN_KEY_CHECKS = 1");
        $db->query("SET @DISABLE_TRIGGERS=NULL");

        closedir($handle);
        rmdir($tempdir);
    }
}
