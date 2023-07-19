<?php
/*
Phidias\Db\Update

Es una clase para escribir queries UPDATE en SQL.
Su unica función es generar un STRING conteniendo un UPDATE válido.

Proporciona herramientas para:
- escribir queries directamente
- interpolar parametros
- construir queries programaticamente
- consturir queries a partir de objetos JSON

Query crudo:
$q = new Update("UPDATE sophia_people SET foo = 'bar' WHERE id = 123");   // No hace nada, mas que encapsular la declaracion del query.

// es literalmente lo mismo que:
$q = "UPDATE sophia_people SET foo = 'bar' WHERE id = 123";

Interpolar parametros (en cadenas)
$q = new Update(
    "UPDATE sophia_people SET foo = 'bar' WHERE id = :personId",
    ["personId" => 123]
)

Construccion programatica

Las palabras que son parte de la sintaxis ("SET", "LIMIT" ...)
tienen una funcion correspondiente (->set(), ->limit(), ...)

En principio, para toda palabra que hace parte de la SINTAXIS SQL
existe una funcion/orden de argumentos correspondiente en la clase Update:


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

use Phidias\Db\Sql\Vm;

class Update
{
    private $table;
    private $values = [];
    private $where;
    private $limit;

    private $params; // OBJECT of params
    private $raw;
    private $vm;

    public function __construct($rawSql = null, $params = null)
    {
        $this->params = new \stdClass;
        if ($params) {
            $this->params($params);
        }

        if (gettype($rawSql) == 'object' || gettype($rawSql) == 'array') {
            $this->parseUpdateObject($rawSql);
        } else {
            $this->raw = $rawSql;
        }

        $this->vm = new Vm();
    }

    public function params($params)
    {
        $objParams = json_decode(json_encode($params));
        foreach ($objParams as $name => $value) {
            $this->params->$name = $value;
        }
        return $this;
    }

    public function table($tableName)
    {
        $this->table = $tableName;
        return $this;
    }

    public function set($columnName, $newValue)
    {
        $this->values[$columnName] = $newValue;
        return $this;
    }

    public function where($stmtWhere, $params = null)
    {
        $this->where = Utils::bindParameters($stmtWhere, $params);
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

    public function limit($limit)
    {
        $this->limit = $limit;
        return $this;
    }

    public function __toString()
    {
        if ($this->raw) {
            return Utils::bindParameters($this->raw, $this->params);
        }

        // Table
        $table = $this->table;

        // Target values
        $setValues = '';
        $newValues = [];
        foreach ($this->values as $columnName => $newValue) {
            $newValues[] = $columnName . " = " . Utils::sanitize($newValue);
        }
        $setValues = implode(", \n", $newValues);

        // WHERE
        $where = '';
        if ($this->where) {
            $incomingWhere = $this->vm->evaluate($this->where);
            if ($incomingWhere) {
                $where = "WHERE ($incomingWhere)";
            }
        }

        // LIMIT
        $limit = '';
        if ($this->limit) {
            $limit = "LIMIT {$this->limit}";
        }

        // cosmetics
        $where = $where ? "\n$where" : '';
        $limit = $limit ? "\n$limit" : '';

        $retval = "UPDATE {$table} \nSET {$setValues} {$where} {$limit}";

        // Bind global params
        $retval = Utils::bindParameters($retval, $this->params);

        return $retval;
    }

    /*
    Load query from a JSON object

    Example:
    {
      "table": "sophia_people",
      "set": {
        "title" => "`id + 2`"
      }
      "where": "record.tableId = 'academic-classbook' AND academic_course.year = :yearId",
      "limit": 20,
    }
    */
    private function parseUpdateObject($objUpdate)
    {
        if (is_string($objUpdate)) {
            return;
        }

        $objUpdate = json_decode(json_encode($objUpdate)); // convert array to object

        if (!isset($objUpdate->table)) {
            throw new \Exception("'table' is required");
        }

        if (!isset($objUpdate->set)) {
            throw new \Exception("'set' is required");
        }

        if (isset($objUpdate->params)) {
            $this->params($objUpdate->params);
        }

        // TABLE
        $this->table($objUpdate->table);

        // NEW VALUES
        foreach ($objUpdate->set as $columnName => $newValue) {
            $this->set($columnName, $newValue);
        }

        // WHERE
        if (isset($objUpdate->where)) {
            $this->where($objUpdate->where);
        }

        // LIMIT
        if (isset($objUpdate->limit)) {
            $this->limit($objUpdate->limit);
        }
    }
}
