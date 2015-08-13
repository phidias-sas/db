<?php namespace Phidias\Db\Orm\Entity;

class Reflection
{
	public static function getEntities($folder)
	{
		$classNames = array();

        $librariesFolder = new \RecursiveDirectoryIterator($folder);
        $iterator        = new \RecursiveIteratorIterator($librariesFolder);
        $files           = new \RegexIterator($iterator, "/.*Entity\.php/", \RegexIterator::GET_MATCH);

        foreach ($files as $file) {

            $filename  = $file[0];
            $className = str_replace(array($folder, ".php", "/"), array("", "", "\\"), $filename);

            if (!class_exists($className)) {
                include $filename;
            }

            $classNames[] = $className;
        }

        return self::organize($classNames);
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
}