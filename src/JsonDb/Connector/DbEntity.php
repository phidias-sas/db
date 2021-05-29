<?php

namespace Phidias\Db\JsonDb\Connector;

class DbEntity implements \Phidias\Db\JsonDb\DatabaseInterface
{
    public function __construct($settings = null)
    {
    }

    public function getTable($tableName)
    {
        return new DbEntityTable($tableName);
    }

    // Pues este es el mimso metodo que Phidias\Db\JsonDb\Database->query
    // tal vez deberia estar en una clase en vez de una interfaz ?
    public function query($query)
    {
        $dataset = new \Phidias\Db\JsonDb\Dataset;
        $dataset->addDatabase('default', $this);

        return $dataset->query($query);
    }
}
