<?php namespace Phidias\Db\Orm\Entity;

class Reflection
{
	public static function getEntities($folder)
	{
		$classnames = array();

        $librariesFolder = new \RecursiveDirectoryIterator($folder);
        $iterator        = new \RecursiveIteratorIterator($librariesFolder);
        $files           = new \RegexIterator($iterator, "/.*Entity\.php/", \RegexIterator::GET_MATCH);

        foreach ($files as $file) {

            $filename  = $file[0];
            $classname = self::getClassName($filename);

            if ($classname === null) {
                continue;
            }

            if (!class_exists($classname)) {
                include $filename;
            }

            $classnames[] = $classname;
        }

        return self::organize($classnames);
	}

    private static function organize($classnames, &$organized = array(), &$checking = array())
    {
        foreach ($classnames as $classname) {

            $classname = trim($classname, "\\");

            if (isset($checking[$classname])) {
                continue;
            }

            $checking[$classname] = true;

            if (!class_exists($classname)) {
                continue;
            }

            if (!is_subclass_of($classname, "\Phidias\Db\Orm\Entity")) {
                continue;
            }

            self::organize($classname::getRelations(), $organized, $checking);

            $organized[] = $classname;

        }

        return $organized;
    }

    private static function getClassName($filename)
    {
        $contents = file_get_contents($filename);

        $matches = [];
        preg_match("/namespace (.+);/", $contents, $matches);
        if (!isset($matches[1])) {
            return;
        }

        $namespace = $matches[1];

        $matches = [];
        preg_match("/class ([^ {]+)/", $contents, $matches);
        if (!isset($matches[1])) {
            return;
        }

        $classname = $matches[1];

        return "$namespace\\$classname";
    }
}