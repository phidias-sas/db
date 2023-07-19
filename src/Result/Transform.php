<?php

namespace Phidias\Db\Result;

class Transform
{
    private $column;
    private $callable;

    public function __construct($column, $callable)
    {
        $this->column = $column;
        $this->callable = $callable;
    }

    public function getColumn()
    {
        return $this->column;
    }

    public function execute($row)
    {
        if (!isset($row[$this->column])) {
            throw new \Exception("Transform error: column '$this->column' not found");
        }

        $fnName = $this->callable;
        return $fnName($row[$this->column], $row);
    }
}
