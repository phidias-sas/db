<?php namespace Phidias\Db\Table;

use Phidias\Db\Db;

/**
 * A schema describing a table in the database
 *
 */
class Schema
{
    private $db; //database identifier

    private $table;
    private $keys;
    private $attributes;
    private $foreignKeys;
    private $triggers;
    private $indexes;
    private $uniques;

    public function __construct()
    {
        $this->db          = null;
        $this->table       = null;
        $this->keys        = array();
        $this->attributes  = array();
        $this->foreignKeys = array();
        $this->triggers    = array();
        $this->indexes     = array();
        $this->uniques     = array();
    }

    public function db($db)
    {
        $this->db = $db;

        return $this;
    }

    public function table($table)
    {
        $this->table = $table;

        return $this;
    }

    /**
     * Define an attribute with the given data
     *
     * $attributeData is a named array which may contain the following keys:
     *
     * array(
     *     'column'        => '',                  //corresponding column in the database.  Defaults to attribute name
     *     'type'          => 'varchar',           //Attribute's SQL type
     *     'length'        => 1,                   //column type length. Defaults to Db Engine default
     *     'autoIncrement' => false,               //use an AUTO_INCREMENT key.  Defaults to false
     *     'unsigned'      => true,                //use UNSIGNED for numeric field.  Defaults to Db Engine default (FALSE)
     *     'acceptNull'    => true,                //Defines if the column can be null.  Defaults to Db Engine default (FALSE)
     *     'default'       => null                 //Default value.  Defaults to Db Engine default (NONE)
     * )
     *
     */
    public function attribute($attributeName, array $attributeData)
    {
        if (!isset($attributeData["type"])) {
            trigger_error("no type specified for attribute '$attributeName'", E_USER_ERROR);
        }

        if (!isset($attributeData["length"])) {
            $attributeData["length"] = null;
        }

        if (!isset($attributeData["column"])) {
            $attributeData["column"] = $attributeName;
        }

        if (!isset($attributeData["autoIncrement"])) {
            $attributeData["autoIncrement"] = false;
        }

        if (!isset($attributeData["unsigned"])) {
            $attributeData["unsigned"] = false;
        }

        if (!isset($attributeData["acceptNull"])) {
            $attributeData["acceptNull"] = false;
        }

        $this->attributes[$attributeName] = $attributeData;

        return $this;
    }


    /**
     * Specifiy which of the created attributes are the primary key
     *
     * $schema->keys(array("key1", "key2"));
     * $schema->keys("key1", "key2");
     */
    public function primaryKey($keys = null)
    {
        if (is_array($keys)) {
            $this->keys = $keys;

            return $this;
        }

        foreach (func_get_args() as $attributeName) {
            if (!isset($this->attributes[$attributeName])) {
                trigger_error("cannot set key: '$attributeName' is not a defined attribute", E_USER_ERROR);
            }

            $this->keys[] = $attributeName;
        }

        return $this;
    }

    /**
     * Specify an attribute as a foreign key.
     *
     * Relation data must be an array:
     * array(
     *     'table'         => '', //foreign table
     *     'column'        => '', //foreign column
     *     'onDelete'      => 'CASCADE'|'UPDATE'|'RESTRICT',
     *     'onUpdate'      => 'CASCADE'|'UPDATE'|'RESTRICT'
     * )
     *
     */
    public function foreignKey($attributeName, array $foreignKeyData)
    {
        if (!isset($this->attributes[$attributeName])) {
            trigger_error("cannot set foreign key: '$attributeName' is not a defined attribute", E_USER_ERROR);
        }

        if (!isset($foreignKeyData["table"])) {
            trigger_error("cannot set foreign key: no foreign table specified", E_USER_ERROR);
        }

        if (!isset($foreignKeyData["column"])) {
            trigger_error("cannot set foreign key: no foreign column specified", E_USER_ERROR);
        }

        if (!isset($foreignKeyData["onDelete"])) {
            $foreignKeyData["onDelete"] = "RESTRICT";
        }

        if (!isset($foreignKeyData["onUpdate"])) {
            $foreignKeyData["onUpdate"] = "RESTRICT";
        }

        $this->foreignKeys[$attributeName] = $foreignKeyData;

        return $this;
    }

    /**
     * Set the given attributeName(s) as an unique key
     *
     * $schema->unique($uniqueAttributes)
     */
    public function unique($uniqueAttributes)
    {
        $uniqueAttributes = (array) $uniqueAttributes;

        foreach ($uniqueAttributes as $attributeName) {
            if (!isset($this->attributes[$attributeName])) {
                trigger_error("cannot set unique key: '$attributeName' is not a defined attribute", E_USER_ERROR);
            }
        }

        $this->uniques[] = $uniqueAttributes;

        return $this;
    }

    /**
     * Set the given attributeName(s) as an index
     *
     * $schema->index($indexName, $indexedAttributes)
     *
     */
    public function index($indexName, $indexedAttributes)
    {
        $indexedAttributes = (array) $indexedAttributes;

        foreach ($indexedAttributes as $attributeName) {
            if (!isset($this->attributes[$attributeName])) {
                trigger_error("cannot set index: '$attributeName' is not a defined attribute", E_USER_ERROR);
            }
        }

        $this->indexes[$indexName] = $indexedAttributes;

        return $this;
    }


    /**
     * Create a trigger for the given time (trigger_time) and event (trigger_even)
     *
     * The trigger name is determined automatically
     *
     * $schema->trigger("before", "insert", $statement);
     * or
     * $schema->trigger("before", "insert", $differentTable, $statement);
     *
     */
    public function trigger($triggerTime, $triggerEvent, $triggerTable, $triggerSQL = null)
    {
        $triggerTime  = strtolower($triggerTime);
        $triggerEvent = strtolower($triggerEvent);

        if ($triggerSQL === null) {
            $triggerSQL   = $triggerTable;
            $triggerTable = $this->table;
        }

        if (!isset($this->triggers[$triggerTable][$triggerTime][$triggerEvent])) {
            $this->triggers[$triggerTable][$triggerTime][$triggerEvent] = array();
        }

        $this->triggers[$triggerTable][$triggerTime][$triggerEvent][] = $triggerSQL;

        return $this;
    }


    public function getTable()
    {
        return $this->table;
    }

    public function getDb()
    {
        return $this->db;
    }

    public function getKeys()
    {
        return $this->keys;
    }

    public function getFirstKey()
    {
        return $this->keys[0];
    }

    public function getAutoIncrementColumns()
    {
        $retval = array();
        foreach ($this->keys as $attributeName) {
            if ($this->isAutoIncrement($attributeName)) {
                $retval[] = $this->getColumn($attributeName);
            }
        }

        return $retval;
    }

    public function isKey($attributeName)
    {
        return in_array($attributeName, $this->keys);
    }

    public function hasAttribute($attributeName)
    {
        return isset($this->attributes[$attributeName]);
    }

    public function getAttributes()
    {
        return $this->attributes;
    }

    public function getAttribute($attributeName)
    {
        return isset($this->attributes[$attributeName]) ? $this->attributes[$attributeName] : null;
    }

    public function getColumn($attributeName)
    {
        return isset($this->attributes[$attributeName]) ? $this->attributes[$attributeName]["column"] : null;
    }

    public function isAutoIncrement($attributeName)
    {
        return isset($this->attributes[$attributeName]["autoIncrement"]) ? $this->attributes[$attributeName]["autoIncrement"] : false;
    }

    public function acceptsNull($attributeName)
    {
        return isset($this->attributes[$attributeName]["acceptNull"]) ? $this->attributes[$attributeName]["acceptNull"] : false;
    }

    public function hasForeignKey($relationName)
    {
        return isset($this->foreignKeys[$relationName]);
    }

    public function getForeignKey($relationName)
    {
        return $this->hasForeignKey($relationName) ? $this->foreignKeys[$relationName] : null;
    }

    /**
     * Get all relations
     *
     * If a $table argument is present, only relations to that table are returned
     */
    public function getForeignKeys($tableName = null)
    {
        if ($tableName === null) {
            return $this->foreignKeys;
        }

        $retval = array();

        foreach ($this->foreignKeys as $relationName => $foreignKeyData) {
            if ($foreignKeyData["table"] == $tableName) {
                $retval[$relationName] = $foreignKeyData;
            }
        }

        return $retval;
    }

    public function getTriggers()
    {
        return $this->triggers;
    }

    public function getIndexes()
    {
        return $this->indexes;
    }

    public function getUniques()
    {
        return $this->uniques;
    }

    //SQL generators
    public function getCreateSQL($engine = 'InnoDb', $charset = 'utf8', $collation = 'utf8_general_ci')
    {
        $query = "CREATE TABLE IF NOT EXISTS `$this->table` ( \n";

        foreach ($this->attributes as $columnData) {

            $columnName         = $columnData['column'];
            $columnType         = $columnData['type'].( isset($columnData['length']) ? '('.$columnData['length'].')' : null );
            $columnUnsigned     = isset($columnData['unsigned']) && $columnData['unsigned'] ? 'unsigned' : null;
            $columnNull         = isset($columnData['acceptNull']) && $columnData['acceptNull'] ? 'NULL' : 'NOT null';

            if (array_key_exists('default', $columnData)) {
                $defaultValue  = is_null($columnData['default']) ? 'NULL' : "'".$columnData['default']."'";
                $columnDefault = "DEFAULT $defaultValue";
            } else {
                $columnDefault  = null;
            }

            $columnIncrement    = isset($columnData['autoIncrement']) && $columnData['autoIncrement'] ? "AUTO_INCREMENT" : null;

            $query .= "\t`$columnName` $columnType $columnUnsigned $columnNull $columnDefault $columnIncrement, \n";
        }

        $primaryKeyColumns = array();

        foreach ($this->keys as $attributeName) {
            $primaryKeyColumns[] = $this->attributes[$attributeName]["column"];
        }
        $query .= "\tPRIMARY KEY (`".implode('`, `', $primaryKeyColumns)."`)";


        $constraintQueries  = array();

        foreach ($this->foreignKeys as $attributeName => $foreignKeyData) {
            $columnName = $this->attributes[$attributeName]["column"];
            $query      .= ",\n\tKEY `$columnName` (`$columnName`)";
        }

        $query .= "\n) ENGINE=$engine CHARACTER SET $charset COLLATE $collation;";

        return $query;
    }

    public function getForeignKeysSQL()
    {
        if (!$this->foreignKeys) {
            return null;
        }

        $foreignKeyStatements = array();

        $cont = 0;

        foreach ($this->foreignKeys as $attributeName => $foreignKeyData) {

            $cont++;

            $columnName     = $this->attributes[$attributeName]["column"];
            $onDelete       = isset($foreignKeyData['onDelete']) ? "ON DELETE ".$foreignKeyData['onDelete'] : null;
            $onUpdate       = isset($foreignKeyData['onUpdate']) ? "ON UPDATE ".$foreignKeyData['onUpdate'] : null;
            $constraintName = "{$this->table}_fk{$cont}";

            $foreignKeyStatements[] = "ADD CONSTRAINT `$constraintName` FOREIGN KEY ( `$columnName` ) REFERENCES `{$foreignKeyData['table']}` (`{$foreignKeyData['column']}`) $onDelete $onUpdate";
        }

        return "ALTER TABLE `$this->table` \n".implode(",", $foreignKeyStatements);
    }

    public function getIndexesSQL()
    {
        if (!$this->indexes) {
            return null;
        }

        $indexStatements = array();
        foreach ($this->indexes as $name => $columns) {
            $indexStatements[] = "ADD INDEX `$name` (`".implode("`, `", $columns)."`)";
        }

        return "ALTER TABLE `$this->table` \n".implode(",", $indexStatements);
    }

    public function getUniquesSQL()
    {
        if (!$this->uniques) {
            return null;
        }

        $uniqueStatements = array();
        foreach ($this->uniques as $columns) {
            $columns = (array) $columns;
            $uniqueStatements[] = "ADD UNIQUE (`".implode("`, `", $columns)."`)";
        }

        return "ALTER TABLE `$this->table` \n".implode(",", $uniqueStatements);
    }

    public function getTriggerQueries()
    {
        if (!$this->triggers) {
            return array();
        }

        $cont    = 0;
        $queries = array();

        foreach ($this->triggers as $tableName => $triggers) {

            foreach ($triggers as $when => $actions) {
                foreach ($actions as $operation => $statements) {
                    foreach ($statements as $statement) {

                        $triggerName = "{$tableName}_{$when}_{$operation}_".(++$cont);

                        $queries[] = "DROP TRIGGER IF EXISTS `$triggerName`";

                        $queries[] = "CREATE TRIGGER `$triggerName` $when $operation ON `$tableName`
                            FOR EACH ROW
                            BEGIN
                                IF (@DISABLE_TRIGGERS IS null) then
                                    $statement
                                END IF;
                            END";
                    }

                }
            }

        }

        return $queries;
    }


    //Load from Database
    public static function load($identifier, $tableName)
    {
        $db = Db::connect($identifier);

        try {
            $response = $db->query("DESCRIBE $tableName");
        } catch (\Exception $e) {
            return null;
        }

        $keys = array();

        $schema = (new Schema)
            ->db($identifier)
            ->table($tableName);

        while ($field = $response->fetch_assoc()) {

            $parts     = explode(" ", $field["Type"]);
            $unsigned  = isset($parts[1]) && $parts[1] === "unsigned";
            $fullType  = $parts[0];

            $typeParts = explode("(", $fullType);
            $type      = $typeParts[0];
            $length    = isset($typeParts[1]) ? substr($typeParts[1], 0, -1) : null;

            $attributeData = array(
                "column"        => $field["Field"],
                "type"          => $type,
                "length"        => $length,
                "autoIncrement" => $field["Extra"] === "auto_increment",
                "unsigned"      => $unsigned,
                "acceptNull"    => $field["Null"] === "YES",
                "default"       => $field["Default"],
            );

            if (!$attributeData["acceptNull"] && $attributeData["default"] === null) {
                unset($attributeData["default"]);
            }

            if ($field["Key"] === "PRI") {
                $keys[] = $field["Field"];
            }

            $schema->attribute($field["Field"], $attributeData);
        }

        $schema->primaryKey($keys);



        $response = $db->query("SHOW CREATE TABLE `$tableName`");

        while ($createTable = $response->fetch_assoc()) {

            $foreignKeyMatches = array();
            preg_match_all("/FOREIGN KEY \(`(.+)`\) REFERENCES `(.+)` \(`(.+)`\)( ON DELETE ([a-zA-Z]+))*( ON UPDATE ([a-zA-Z]+))*/", $createTable["Create Table"], $foreignKeyMatches);

            if (!isset($foreignKeyMatches[1])) {
                continue;
            }

            foreach ($foreignKeyMatches[1] as $key => $localColumn) {

                if (!isset($foreignKeyMatches[2][$key]) || !isset($foreignKeyMatches[3][$key])) {
                    continue;
                }

                $foreignKeyData = array(
                    "table"    => $foreignKeyMatches[2][$key],
                    "column"   => $foreignKeyMatches[3][$key],
                    "onDelete" => isset($foreignKeyMatches[5][$key]) && !empty($foreignKeyMatches[5][$key]) ? $foreignKeyMatches[5][$key] : null,
                    "onUpdate" => isset($foreignKeyMatches[7][$key]) && !empty($foreignKeyMatches[7][$key]) ? $foreignKeyMatches[7][$key] : null,
                );

                $schema->foreignKey($localColumn, $foreignKeyData);
            }

        }

        return $schema;
    }


    //Write operations on the database
    public function defragment()
    {
        $db = Db::connect($this->db);
        $db->query("ALTER TABLE `$this->table` ENGINE = InnoDb");
    }

    public function optimize()
    {
        $db = Db::connect($this->db);
        $db->query("OPTIMIZE TABLE `$this->table`");
    }

    public function create()
    {
        $db = Db::connect($this->db);

        $db->query($this->getCreateSQL());

        if ($foreignKeysSQL = $this->getForeignKeysSQL()) {
            $db->query($foreignKeysSQL);
        }

        if ($indexesSQL = $this->getIndexesSQL()) {
            $db->query($indexesSQL);
        }

        if ($uniquesSQL = $this->getUniquesSQL()) {
            $db->query($uniquesSQL);
        }
    }

    public function createTriggers()
    {
        if ($triggerQueries = $this->getTriggerQueries()) {
            $db = Db::connect($this->db);

            foreach ($triggerQueries as $query) {
                $db->query($query);
            }
        }
    }

    public function drop()
    {
        $db = Db::connect($this->db);
        $db->query("DROP TABLE IF EXISTS `$this->table`");
    }

    public function truncate()
    {
        $db = Db::connect($this->db);
        $db->query("TRUNCATE `$this->table`");
    }

    public function delete()
    {
        $db = Db::connect($this->db);
        $db->query("DELETE FROM `$this->table`");
        $db->query("ALTER TABLE `$this->table` AUTO_INCREMENT = 1");
    }


    /**
     * If the schema's table exists in the database patch it to correspond to this schema's definition.
     * Create it otherwise
     * 
     */
    public function patch()
    {
        $currentSchema = self::load($this->db, $this->table);

        if ($currentSchema === null) {
            return $this->create();
        }

        $currentSchema->alterTo($this);
    }


    public function alterTo($targetSchema)
    {
        $db = Db::connect($this->db);

        foreach ($this->attributes as $attributeName => $attributeData) {
            if (!isset($targetSchema->attributes[$attributeName])) {
                $db->query("ALTER TABLE `$this->table` DROP `$attributeName`");
            }
        }

        $previousAttribute = null;

        foreach ($targetSchema->attributes as $attributeName => $attributeData) {

            $typeString      = $attributeData["type"] . (isset($attributeData["length"]) ? "({$attributeData["length"]})": "");
            $unsignedString  = isset($attributeData["unsigned"]) && $attributeData["unsigned"] ? "unsigned" : null;
            $nullString      = isset($attributeData["acceptNull"]) && $attributeData["acceptNull"] ? "NULL": "NOT NULL";
            $incrementString = isset($attributeData["autoIncrement"]) && $attributeData["autoIncrement"] ? "AUTO_INCREMENT" : null;

            if (array_key_exists("default", $attributeData)) {
                $defaultValue  = is_null($attributeData["default"]) ? 'NULL' : "'".$attributeData["default"]."'";
                $defaultString = "DEFAULT $defaultValue";
            } else {
                $defaultString  = null;
            }

            if (!isset($this->attributes[$attributeName])) {
                $positionString = $previousAttribute == null ? "FIRST" : "AFTER `$previousAttribute`";
                $db->query("ALTER TABLE `$this->table` ADD `$attributeName` $typeString $unsignedString $nullString $defaultString $incrementString $positionString;");
                continue;
            }

            $isDifferent = false;

            foreach ($attributeData as $property => $value) {

                if ($property == "type" && $value == "integer") {
                    $value = "int";
                }

                if ($property == "length" && $value == null) {
                    continue;
                }

                if (isset($this->attributes[$attributeName][$property]) && $this->attributes[$attributeName][$property] != $value) {
                    $isDifferent = true;
                    break;
                }
            }

            if ($isDifferent) {
                $db->query("ALTER TABLE `$this->table` CHANGE `$attributeName` `$attributeName` $typeString $unsignedString $nullString $defaultString $incrementString;");
            }

            $previousAttribute = $attributeName;
        }

    }

}
