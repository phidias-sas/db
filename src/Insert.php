<?php
/*
Phidias\Db\Insert

Es una clase para escribir queries INSERT en SQL.
Su unica función es generar un STRING conteniendo un INSERT válido.

Proporciona herramientas para:
- escribir queries directamente
- interpolar parametros
- construir queries programaticamente
- consturir queries a partir de objetos JSON

Query crudo:
$q = new Insert("INSERT sophia_people SET foo = 'bar' WHERE id = 123");   // No hace nada, mas que encapsular la declaracion del query.

// es literalmente lo mismo que:
$q = "INSERT sophia_people SET foo = 'bar' WHERE id = 123";

Interpolar parametros (en cadenas)
$q = new Insert(
    "INSERT sophia_people SET foo = 'bar' WHERE id = :personId",
    ["personId" => 123]
)

Construccion programatica

Las palabras que son parte de la sintaxis ("SET", "LIMIT" ...)
tienen una funcion correspondiente (->set(), ->limit(), ...)

En principio, para toda palabra que hace parte de la SINTAXIS SQL
existe una funcion/orden de argumentos correspondiente en la clase Insert:


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

class Insert
{
    private $table;
    private $values = [];

    private $params; // OBJECT of params
    private $raw;

    public function __construct($rawSql = null, $params = null)
    {
        $this->params = new \stdClass;
        if ($params) {
            $this->params($params);
        }

        if (gettype($rawSql) == 'object' || gettype($rawSql) == 'array') {
            $this->parseInsertObject($rawSql);
        } else {
            $this->raw = $rawSql;
        }
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

        $retval = "INSERT INTO {$table} \nVALUES {$setValues}";

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
    private function parseInsertObject($objInsert)
    {
        if (is_string($objInsert)) {
            return;
        }

        $objInsert = json_decode(json_encode($objInsert)); // convert array to object

        if (!isset($objInsert->table)) {
            throw new \Exception("'table' is required");
        }

        if (!isset($objInsert->set)) {
            throw new \Exception("'set' is required");
        }

        if (isset($objInsert->params)) {
            $this->params($objInsert->params);
        }

        // TABLE
        $this->table($objInsert->table);

        // NEW VALUES
        foreach ($objInsert->set as $columnName => $newValue) {
            $this->set($columnName, $newValue);
        }

        // WHERE
        if (isset($objInsert->where)) {
            $this->where($objInsert->where);
        }

        // LIMIT
        if (isset($objInsert->limit)) {
            $this->limit($objInsert->limit);
        }
    }
}
