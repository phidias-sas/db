<?php
/*
Phidias\Db\Select

Es una clase para escribir queries SELECT en SQL.
Su unica función es generar un STRING conteniendo un SELECT válido.

Proporciona herramientas para:
- escribir queries directamente
- interpolar parametros
- construir queries programaticamente
- consturir queries a partir de objetos JSON

Query crudo:
$q = new Select("SELECT * FROM sophia_people");   // No hace nada, mas que encapsular la declaracion del query.

// es literalmente lo mismo que:
$q = "SELECT * FROM sophia_people";


Interpolar parametros (en cadenas)
$q = new Select(
    "SELECT * FROM sophia_people WHERE lastname LIKE :comparacion",
    ["comparacion" => "%" . $searchName]
)

Construccion programatica

Las palabras que son parte de la sintaxis ("FROM", "INNER JOIN", "LEFT JOIN", "WHERE", "ORDER BY", ...)
tienen una funcion correspondiente (->from(), ->innerJoin(), ->leftJoin(), ->where(), ->orderBy(), ...)

En principio, para toda palabra que hace parte de la SINTAXIS SQL (incluye "ON", "AS", "DESC")
existe una funcion/orden de argumentos correspondiente en la clase Select:


$q = new Select()                                                           SELECT

    ->column("person.id")                                                   person.id,
    ->column("clinic.field1")                                               clinic.field1,
    ->column("clinic.field2")                                               clinic.field2,
    ->column("clinic.field3")                                               clinic.field3,
    ->column("CONCAT(a,b,c)", "fullThing")                                  CONCAT(a,b,c) as `fullThing`,

    ->column([
        "person.id",                                                        person.id,
        "clinic.field1",                                                    clinic.field1,
        "clinic.field2",                                                    clinic.field2,
        "clinic.field3"                                                     clinic.field3
        "CONCAT(a,b,c)" => "fullThing"                                      CONCAT(a,b,c) as `fullThing`,
    ])

    ->from("sophia_people")                                                 FROM sophia_people
    ->from("sophia_people", "person")                                       FROM sophia_people `person`

    ->leftJoin("sophia_clinic clinic ON clinic.person = person.id")         LEFT JOIN sophia_clinic clinic ON clinic.person = person.id
    ->leftJoin("sophia_clinic", "clinic", "clinic.person = person.id")      LEFT JOIN sophia_clinic `clinic` ON clinic.person = person.id

    ->where("clinic.field1 = :var1", ["var1" => "pepe])                     WHERE clinic.field1 = 'pepe'
    ->orderBy("clinic.field1 DESC")                                         ORDER BY clinic.field1 DESC
    ->limit(50)                                                             LIMIT 50
    ->limit("50, 100")                                                      LIMIT 50, 100
*/

namespace Phidias\Db;

use Phidias\Db\Select\Loop;
use Phidias\Db\Select\Transform;
use Phidias\Db\Select\Utils;
use Phidias\Db\Select\Vm;

class Select
{
    private $fromTable;
    private $fromAlias;
    private $columns;
    private $joins;
    private $where;
    private $having;
    private $groupBy;
    private $orderBy;
    private $limit;

    private $params; // OBJECT of params
    private $raw;
    private $vm;
    private $outputIterator;

    private static $_replaceArrowFunctions = false;

    public function __construct($rawSql = null, $params = null)
    {
        $this->columns = [];
        $this->joins = [];
        $this->params = new \stdClass;
        if ($params) {
            $this->params($params);
        }

        if (gettype($rawSql) == 'object' || gettype($rawSql) == 'array') {
            $this->parseSelectObject($rawSql);
        } else {
            $this->raw = $rawSql;
        }

        $this->vm = new Vm();
    }

    public static function loop($iteratorKey, $iteratorOutput)
    {
        return new Loop($iteratorKey, $iteratorOutput);
    }

    public static function transform($column, $callable)
    {
        return new Transform(self::replaceJsonArrowOperator($column), $callable);
    }

    public function params($params)
    {
        $objParams = json_decode(json_encode($params));
        foreach ($objParams as $name => $value) {
            $this->params->$name = $value;
        }
        return $this;
    }

    public function from($tableName, $tableAlias = null)
    {
        $this->fromTable = $tableName;
        $this->fromAlias = $tableAlias;
        return $this;
    }

    public function column($columnSrc, $columnAlias = null, $batchAlias = null)
    {
        if (is_array($columnSrc) || is_object($columnSrc)) {
            foreach ($columnSrc as $propName => $propValue) {
                if (is_numeric($propName)) {
                    $this->column($propValue);
                } else {
                    $this->column($propName, $propValue);
                }
            }
            return $this;
        }

        $this->columns[] = (object)[
            "src" => $columnSrc,
            "alias" => $columnAlias,
        ];
        return $this;
    }

    public function where($stmtWhere, $params = null)
    {
        $this->where = Utils::bindParameters($stmtWhere, $params);
        return $this;
    }

    public function having($stmtHaving, $params = null)
    {
        $this->having = Utils::bindParameters($stmtHaving, $params);
        return $this;
    }

    public function and($stmt, $params = null)
    {
        $sanitizedStmt = Utils::bindParameters($stmt, $params);

        if ($this->where) {
            $this->where = (object)[
                "and" => [$this->where, $sanitizedStmt]
            ];
        } else {
            $this->where = $sanitizedStmt;
        }

        return $this;
    }

    public function or($stmt, $params = null)
    {
        $sanitizedStmt = Utils::bindParameters($stmt, $params);

        if ($this->where) {
            $this->where = (object)[
                "or" => [$this->where, $sanitizedStmt]
            ];
        } else {
            $this->where = $sanitizedStmt;
        }

        return $this;
    }

    public function orderBy($orderBy, $desc = null)
    {
        $this->orderBy = $orderBy;
        if ($desc) {
            $this->orderBy .= ' DESC';
        }
        return $this;
    }

    public function groupBy($groupBy)
    {
        $this->groupBy = $groupBy;
        return $this;
    }

    public function limit($limitA, $limitB = null)
    {
        $this->limit = $limitB ? "$limitA, $limitB" : $limitA;
        return $this;
    }

    public function innerJoin($tableName, $tableAlias, $joinOn, $params = null)
    {
        return $this->appendJoin('INNER JOIN', $tableName, $tableAlias, $joinOn, $params = null);
    }

    public function leftJoin($tableName, $tableAlias, $joinOn, $params = null)
    {
        return $this->appendJoin('LEFT JOIN', $tableName, $tableAlias, $joinOn, $params = null);
    }

    public function rightJoin($tableName, $tableAlias, $joinOn, $params = null)
    {
        return $this->appendJoin('RIGHT JOIN', $tableName, $tableAlias, $joinOn, $params = null);
    }

    public function fullJoin($tableName, $tableAlias, $joinOn, $params = null)
    {
        return $this->appendJoin('FULL JOIN', $tableName, $tableAlias, $joinOn, $params = null);
    }

    private function appendJoin($type, $tableName, $tableAlias, $joinOn, $params = null)
    {
        $this->joins[] = (object)[
            "type" => $type,
            "table" => $tableName,
            "alias" => $tableAlias,
            "on" => Utils::bindParameters($joinOn, $params)
        ];
        return $this;
    }

    public function output($outputMap)
    {
        if ($outputMap instanceof Loop) {
            $this->outputIterator = $this->applyIterator($outputMap);
        } else {
            $this->outputIterator = $this->applyIterator(new Loop(null, $outputMap));
        }

        return $this;
    }

    public function __toString()
    {
        if ($this->raw) {
            return Utils::bindParameters($this->raw, $this->params);
        }

        // SELECT COLUMNS
        $select = '';
        $selectItems = [];
        $seenAliases = [];

        foreach ($this->columns as $column) {
            $finalColumnName = $column->alias ? $column->alias : $column->src;
            if (isset($seenAliases[$finalColumnName])) {
                continue;
            }
            $seenAliases[$finalColumnName] = true;

            if (!$column->alias) {
                $selectItems[] = '  ' . $column->src;
            } else {
                $selectItems[] = "  {$column->src} as `{$column->alias}`";
            }
        }
        $select = implode(", \n", $selectItems);

        // FROM
        $from = $this->fromAlias
            ? "$this->fromTable as `{$this->fromAlias}`"
            : $this->fromTable;

        // JOIN
        $join = '';
        $joinStatements = [];
        foreach ($this->joins as $joinItem) {
            $joinType = $joinItem->type;
            $joinTable = $joinItem->table;
            $joinAs = $joinItem->alias;
            $joinOn = $joinItem->on;
            $joinStatements[] = "{$joinType} {$joinTable} as `{$joinAs}` ON {$joinOn}";
        }
        if (count($joinStatements)) {
            $join = implode("\n", $joinStatements);
        }

        // WHERE
        $where = '';
        if ($this->where) {
            $incomingWhere = $this->vm->evaluate($this->where);
            if ($incomingWhere) {
                $where = "WHERE ($incomingWhere)";
            }
        }

        // GROUP BY
        $groupBy = '';
        if ($this->groupBy) {
            $groupBy = 'GROUP BY ' . $this->groupBy;
        }

        // HAVING
        $having = '';
        if ($this->having) {
            $having = 'HAVING ' . $this->having;
        }

        // ORDER BY
        $orderBy = '';
        if ($this->orderBy) {
            if (is_array($this->orderBy)) {
                $orderBy = 'ORDER BY ';
                foreach ($this->orderBy as $orderItem) {
                    $direction = isset($orderItem->desc) && $orderItem->desc ? "DESC" : "ASC";
                    $orderBy .= "{$orderItem->column} {$direction}, ";
                }
                $orderBy = rtrim($orderBy, ", ");
            } else if (is_string($this->orderBy)) {
                $orderBy = 'ORDER BY ' . $this->orderBy;
            }
        }

        // LIMIT
        $limit = '';
        if ($this->limit) {
            $limit = "LIMIT {$this->limit}";
        }

        // cosmetics
        $join = $join ? "\n$join" : '';
        $where = $where ? "\n$where" : '';
        $groupBy = $groupBy ? "\n$groupBy" : '';
        $having = $having ? "\n$having" : '';
        $orderBy = $orderBy ? "\n$orderBy" : '';
        $limit = $limit ? "\n$limit" : '';

        $retval = "SELECT \n{$select} \nFROM {$from} {$join} {$where} {$groupBy} {$having} {$orderBy} {$limit}";

        // Bind global params
        $retval = Utils::bindParameters($retval, $this->params);

        // ->>'$.xxxx' syntax polyfill
        $retval = self::replaceJsonArrowOperator($retval);

        return $retval;
    }

    private function applyIterator(Loop $iterator, $prefix = '')
    {
        if ($iterator->key) {
            $this->column($iterator->key, $iterator->key);
        }
        $iterator->output = $this->applyColumns($iterator->output, $prefix);
        return $iterator;
    }

    private function applyColumns($objOutput, $prefix = '')
    {
        $retval = (object)[];
        foreach ($objOutput as $propName => $propSource) {
            $columnAlias = $prefix ? $prefix . '.' . $propName : $propName;

            if (is_string($propSource)) {
                $this->column($propSource, $columnAlias);
                $retval->$propName = $columnAlias;
            } else if ($propSource instanceof Loop) {
                $retval->$propName = $this->applyIterator($propSource, $columnAlias);
            } else if ($propSource instanceof Transform) {
                $sourceColumn = $propSource->getColumn();
                $this->column($sourceColumn, $sourceColumn);
                $retval->$propName = $propSource;
            } else if (is_object($propSource) || is_array($propSource)) {
                $retval->$propName = $this->applyColumns($propSource, $columnAlias);
            } else {
                $retval->$propName = $propSource;
            }
        }

        return $retval;
    }

    public function fetch_all($mysqli_result)
    {
        if ($this->outputIterator) {
            return $this->outputIterator->fetch_all($mysqli_result);
        }

        return $mysqli_result->fetch_all(MYSQLI_ASSOC);
    }


    public function defineOperator($operatorName, $callable)
    {
        $this->vm->defineOperator($operatorName, $callable);
        return $this;
    }

    public static function replaceArrowFunctions($val = true)
    {
        self::$_replaceArrowFunctions = !!$val;
    }

    private static function replaceJsonArrowOperator($query)
    {
        if (!self::$_replaceArrowFunctions) {
            return $query;
        }

        $jsonArrowRegex = '/([a-zA-Z0-9._-]+)\s*->>\s*([\'"])(.+?)\2/';
        preg_match_all($jsonArrowRegex, $query, $matches);
        foreach ($matches[0] as $i => $jsonArrow) {
            $field = $matches[1][$i];
            $quote = $matches[2][$i];
            $path = $matches[3][$i];
            $replacement = "IF(JSON_EXTRACT($field, $quote{$path}$quote) = 'null', NULL, JSON_UNQUOTE(JSON_EXTRACT($field, $quote{$path}$quote)))";
            $query = str_replace($jsonArrow, $replacement, $query);
        }

        return $query;
    }


    /*
    Load query from a JSON object

    Example:
    {
      "params": { "yearId": 26 },
      "from": { "table": "sophia_jsondb_bigtable_records", "as": "record" },
      "join": [
        { "type": "left", "table": "sophia_people", "as": "author", "on": "author.id = record.authorId" },
        { "type": "inner", "table": "sophia_academic_course_group_sessions", "as": "academic_session", "on": "academic_session.id = record.data->>'$.sessionId'" },
        { "type": "inner", "table": "sophia_academic_course_groups", "as": "academic_group", "on": "academic_group.id = record.data->>'$.groupId'" },
        { "type": "inner", "table": "sophia_academic_courses", "as": "academic_course", "on": "academic_course.id = academic_group.course" },
        { "type": "inner", "table": "sophia_academic_subjects", "as": "academic_subject", "on": "academic_subject.id = academic_course.subject" },
        { "type": "inner", "table": "sophia_people", "as": "group_teacher", "on": "group_teacher.id = academic_group.teacher" }
      ],
      "where": "record.tableId = 'academic-classbook' AND academic_course.year = :yearId",
      "orderBy": null,
      "limit": 20,

      "columns": [
        {"column": "mandatory", "as": "optional"},
      ],
    }
    */
    private function parseSelectObject($objSelect)
    {
        if (is_string($objSelect)) {
            return;
        }

        $objSelect = json_decode(json_encode($objSelect)); // convert array to object

        if (!isset($objSelect->from)) {
            throw new \Exception("'from' is required");
        }

        if (isset($objSelect->params)) {
            $this->params($objSelect->params);
        }

        // FROM
        if (is_string($objSelect->from)) {
            $this->from($objSelect->from);
        } else if (isset($objSelect->from->table)) {
            $this->from($objSelect->from->table, $objSelect->from->as ?? null);
        }

        // SELECT COLUMNS
        if (isset($objSelect->iterator->key) && isset($objSelect->iterator->output)) {
            $this->output(
                $this->parseOutput(new Loop($objSelect->iterator->key, $objSelect->iterator->output))
            );
        } else {
            if (!isset($objSelect->columns) || !is_array($objSelect->columns)) {
                throw new \Exception("'columns' must be an array");
            }

            foreach ($objSelect->columns as $objColumn) {
                if (is_string($objColumn)) {
                    $this->column($objColumn);
                } else if (isset($objColumn->column)) {
                    $this->column($objColumn->column, $objColumn->as ?? null);
                }
            }
        }


        // JOIN
        if (isset($objSelect->join)) {
            if (!is_array($objSelect->join)) {
                throw new \Exception("'join' must be an array");
            }

            foreach ($objSelect->join as $joinItem) {
                if (!isset($joinItem->table)) {
                    throw new \Exception("Join item must have 'table'");
                }
                if (!isset($joinItem->as)) {
                    throw new \Exception("Join item must have 'as'");
                }
                if (!isset($joinItem->on)) {
                    throw new \Exception("Join item must have 'on'");
                }

                $joinType = strtolower($joinItem->type ?? '');
                switch ($joinType) {
                    case 'left':
                        $this->leftJoin($joinItem->table, $joinItem->as, $joinItem->on);
                        break;
                    case 'right':
                        $this->rightJoin($joinItem->table, $joinItem->as, $joinItem->on);
                        break;

                    case 'inner':
                    default:
                        $this->innerJoin($joinItem->table, $joinItem->as, $joinItem->on);
                        break;
                }
            }
        }

        // WHERE
        if (isset($objSelect->where)) {
            $this->where($objSelect->where);
        }

        // ORDER BY
        if (isset($objSelect->orderBy)) {
            $this->orderBy($objSelect->orderBy);
        }

        // GROUP BY
        if (isset($objSelect->groupBy)) {
            $this->groupBy($objSelect->groupBy);
        }

        // LIMIT
        if (isset($objSelect->limit)) {
            $this->limit($objSelect->limit);
        }
    }

    private function parseOutput($objOutput)
    {
        if ($objOutput instanceof Loop) {
            return new Loop(
                $objOutput->key,
                $this->parseOutput($objOutput->output)
            );
        } else if (isset($objOutput->transform)) {
            if (!isset($objOutput->function)) {
                throw new \Exception("Invalid transform function for column '$objOutput->transform'");
            }
            return new Transform(
                $this->replaceJsonArrowOperator($objOutput->transform),
                function ($value, $row) use ($objOutput) {
                    return self::runCustomFunction($objOutput->function, $value, $row);
                }
            );
        } else if (is_object($objOutput) || is_array($objOutput)) {
            $retval = new \stdClass;
            foreach ($objOutput as $propName => $propValue) {
                $retval->$propName = $this->parseOutput($propValue);
            }
            return $retval;
        }

        return $objOutput;
    }

    public static function runCustomFunction($functionName, $value, $row)
    {
        switch ($functionName) {
            case 'formatDate':
                return "FORMATTING DATE: $value";
            case 'json_decode':
                return json_decode($value);
            default:
                // Unknown function '$functionName'
                return null;
        }
    }
}
