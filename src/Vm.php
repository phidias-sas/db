<?php

namespace Phidias\Db;

use Phidias\Json\Utils;
use Phidias\Db\Select;
use Phidias\Db\Update;

class Vm extends \Phidias\Json\Vm
{
    private $conn;

    public function __construct($conn, $model = null)
    {
        parent::__construct($model);

        $this->conn = $conn;

        $this->defineStatement('assign', [$this, 'stmtAssign']);
        $this->defineStatement('chain', [$this, 'stmtChain']);
        $this->defineStatement('select', [$this, 'stmtSelect']);
        $this->defineStatement('update', [$this, 'stmtUpdate']);
    }

    /*
    {
        "assign": "target variable name",
        "stmt": " ... vm statement "
    }
    */
    public function stmtAssign($expr)
    {
        $result = isset($expr->stmt)
            ? $this->evaluate($expr->stmt)
            : null;

        if ($expr->assign) {
            $this->setVariable($expr->assign, $result);
        }

        return $result;
    }

    /*
    {
        "chain": [
            ... stmt1,
            ... stmt2,
            ...
        ]
    }
    */
    public function stmtChain($expr)
    {
        if (!isset($expr->chain) || !is_array($expr->chain)) {
            return [];
        }

        $retval = [];
        foreach ($expr->chain as $chainItem) {
            $retval[] = $this->evaluate($chainItem);
        }
        return $retval;
    }

    /*
    */
    public function stmtSelect($expr)
    {
        $parsedStmt = Utils::parse($expr->select, $this->model);
        $q = new Select($parsedStmt);
        $result = $this->conn->query($q);

        return $q->fetch_all($result);
    }

    /*
    */
    public function stmtUpdate($expr)
    {
        $parsedStmt = Utils::parse($expr->update, $this->model);
        $q = new Update($parsedStmt);
        $this->conn->query($q);

        return [
            "affected_rows" => $this->conn->mysqli->affected_rows,
            "info" => $this->conn->mysqli->info,
        ];
    }
}
