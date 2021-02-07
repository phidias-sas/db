<?php namespace Phidias\Db\Orm;
/**
 *
 * Orm Entity
 *
 * En Orm entity is declared by extending this class and declaring a Schema
 * in the static variable $schema:
 *
 * e.g.:
 *
 * myModule/libraries/Book/Entity.php:
 *
 * <?php
 *
 * namespace Book;
 *
 * class Entity extends \Phidias\Db\Orm\Entity
 * {
 *     public $id;
 *     public $title;
 *     .... declare all attributes
 *
 *     //Schema definition:
 *
 *     protected static $schema = array(
 *
 *         'db'    => 'Db_NAME'            //the name of the database (as configured via phidias.db.Db_NAME.property).
 *         'table' => '.....',             //the name of the table
 *         'keys'  => array('id' [, ...]), //array identifying one or more columns as keys
 *
 *         'attributes' => array(
 *
 *             'id' => array(
 *                 'type'          => 'integer',
 *                 'unsigned'      => true,
 *                 'autoIncrement' => true
 *             ),
 *
 *             'title' => array(
 *                 'type'          => 'varchar',
 *                 'length'        => 128,
 *                 'acceptNull'    => true,
 *                 'default'       => null
 *             ),
 *
 *             .... possible declarations:
 *
 *             'attributeName' => array(
 *                 'column'        => '',                  //corresponding column in the database.  Defaults to attribute name
 *                 'length'        => 1,                   //column type length. Defaults to Db Engine default
 *                 'autoIncrement' => false,               //use an AUTO_INCREMENT key.  Defaults to false
 *                 'unsigned'      => true,                //use UNSIGNED for numeric field.  Defaults to Db Engine default (FALSE)
 *                 'acceptNull'    => true,                //Defines if the column can be null.  Defaults to Db Engine default (FALSE)
 *                 'default'       => null                 //Default value.  Defaults to Db Engine default (NONE)
 *             ),
 *
 *             //Declare a foreign key to another entity
 *
 *             'relationName' => array(
 *                 'column'        => '',                  //corresponding column in the database.  Default to attribute name
 *                 "entity"        => 'Author\Entity',     //full class name of the related entity
 *                 'attribute'     => 'id',                //related attribute (usually the related entity's primary key)
 *                 'acceptNull'    => true,
 *                 'default'       => null,
 *
 *                 'onDelete'      => 'CASCADE'|'UPDATE'|'RESTRICT',
 *                 'onUpdate'      => 'CASCADE'|'UPDATE'|'RESTRICT'
 *             )
 *
 *         ),
 *
 *
 *         //Declare column indexes (ADD INDEX):
 *
 *         'indexes' => array(
 *             'lastname1' => 'lastname1',
 *             'lastname2' => 'lastname2',
 *             'username'  => 'username'
 *         ),
 *
 *
 *         //Declare unique indexes (ADD UNIQUE):
 *
 *         'unique' => array(
 *             array('person', 'token')
 *             .... [attribute name, or array of attribute names] ...
 *         ),
 *
 *
 *          //Declare triggers
 *
 *          'triggers' => array(
 *              'table name' => array(
 *                  'operation (insert|update|delete)' => array(
 *                      'when (before|after)' => 'statement'
 *                  )
 *              )
 *          )
 *     );
 *
 *
 * }
 *
 */

use Phidias\Db\Table\Schema;
use Phidias\Db\Iterator;

class Entity
{
    protected static $schema;
    protected static $customConditions;

    public static function defineCondition($name, $callback)
    {
        if (!is_callable($callback)) {
            trigger_error("defineCondition: invalid callback for '$name'", E_USER_ERROR);
        }

        $className = get_called_class();

        if (!isset(self::$customConditions[$className])) {
            self::$customConditions[$className] = [];
        }
        self::$customConditions[$className][$name] = $callback;
    }

    public static function collection($attributesObject = null)
    {
        $className  = get_called_class();
        $schema     = $className::getSchema();

        $collection = new Collection($schema);
        $collection->className($className);
        if (isset(self::$customConditions[$className])) {
            $collection->setCustomConditions(self::$customConditions[$className]);
        }

        //Set the collection attributes from the given object
        if ($attributesObject !== null) {

            foreach (get_object_vars($attributesObject) as $attributeName => $value) {

                if (!$schema->hasAttribute($attributeName)) {
                    continue;
                }

                if (is_a($value, "Phidias\Db\Orm\Entity")) {
                    $collection->attribute($attributeName, $value::collection($value));
                } elseif (is_scalar($value) || is_null($value) || $schema->isJson($attributeName)) {
                    $collection->attribute($attributeName);
                }
            }
        }

        return $collection;
    }

    public static function iterator($keyFields, $hasOneElement = false)
    {
        return new Iterator($keyFields, get_called_class(), $hasOneElement);
    }

    public static function single()
    {
        return self::collection()->hasOneElement();
    }

    public function __construct($id = null)
    {
        if ($id !== null) {
            $this->setValues(self::collection()->allAttributes()->fetch($id));
        }
    }

    public function setValues($values, $acceptedAttributes = null)
    {
        if (!is_array($values) && !is_object($values)) {
            return $this;
        }

        $schema = self::getSchema();

        foreach ($values as $attribute => $value) {

            if (!$schema->hasAttribute($attribute)) {
                continue;
            }

            if ($acceptedAttributes !== null && !in_array($attribute, $acceptedAttributes, true)) {
                continue;
            }

            if ($schema->hasForeignKey($attribute) && (is_array($value)||is_object($value)) && ($relatedEntity = self::getRelation($attribute))) {
                $relatedEntityClassName = $relatedEntity["entity"];
                $this->$attribute     = new $relatedEntityClassName;
                $this->$attribute->setValues($value, $acceptedAttributes);
            } else {
                $this->$attribute = $value;
            }
        }

        return $this;
    }

    public function fetchAll()
    {
        $retval = clone($this);

        foreach (get_object_vars($retval) as $attributeName => $value ) {
            if (is_a($value, '\Phidias\Db\Iterator') || is_a($value, '\Phidias\Db\Orm\Entity')) {
                $retval->$attributeName = $value->fetchAll();
            }
        }

        return $retval;
    }

    public function save()
    {
        self::collection($this)->save($this);
        return $this;
    }

    public function delete()
    {
        $schema     = self::getSchema();
        $collection = self::collection();

        foreach ($schema->getKeys() as $keyAttributeName) {
            if (isset($this->$keyAttributeName)) {
                $collection->match($keyAttributeName, $this->$keyAttributeName);
            }
        }

        return $collection->delete();
    }


    /**
     * Return a collection with its search parameters matching currently set attributes
    */
    public function find()
    {
        $retval = self::collection()->allAttributes();
        $schema = self::getSchema();

        foreach ($schema->getAttributes() as $attributeName => $value) {
            if (isset($this->$attributeName)) {
                $retval->match($attributeName, $this->$attributeName);
            }
        }

        return $retval->find();
    }

    /**
     * Treat currently set attributes as search keys and retrieve the first result.
     * Attempts to create the entity with the currently set attributes if no results are found
    */
    public function obtain()
    {
        $match = $this->find()->first();

        if ($match == null) {
            return $this->save();
        }

        $this->setValues($match);

        return $this;
    }


    /**
     * Return an array of classNames for outgoing foreign keys
     */
    public static function getRelations()
    {
        $className = get_called_class();
        $relations = array();

        foreach ($className::$schema["attributes"] as $attributeName => $attributeData) {
            if (isset($attributeData["entity"])) {
                $relations[$attributeName] = $attributeData["entity"];
            }
        }

        return $relations;
    }

    public static function getRelation($attributeName)
    {
        $className = get_called_class();

        return isset($className::$schema["attributes"][$attributeName]["entity"]) ? $className::$schema["attributes"][$attributeName] : null;
    }

    public static function getUniqueId()
    {
        return self::collection()->getUniqueId();
    }

    public static function getSchema()
    {
        $className = get_called_class();
        $array     = $className::$schema;

        if (!isset($array['table'])) {
            trigger_error('invalid schema: no table defined', E_USER_ERROR);
        }

        if (!isset($array['attributes'])) {
            trigger_error('invalid schema: no attributes defined', E_USER_ERROR);
        }

        if (!isset($array['keys'])) {
            trigger_error('invalid schema: no keys defined', E_USER_ERROR);
        }


        $schemaObject = new Schema();
        $schemaObject->table($array["table"]);
        $schemaObject->primaryKey($array['keys']);

        $foreignKeys = array();

        foreach ($array["attributes"] as $attributeName => $attributeData) {

            if (isset($attributeData["type"]) && $attributeData["type"] == "uuid") {
                $attributeData["type"]   = "varchar";
                $attributeData["length"] = 13;
                $attributeData["uuid"]   = true;
            }


            if (!isset($attributeData["column"])) {
                $attributeData["column"] = $attributeName;
            }

            if (isset($attributeData["entity"])) {

                if (!class_exists($attributeData["entity"])) {
                    trigger_error("invalid schema: related entity '{$attributeData["entity"]}' not found", E_USER_ERROR);
                }

                $foreignClassName     = $attributeData["entity"];
                $foreignSchema        = ($foreignClassName == $className) ? $schemaObject : $foreignClassName::getSchema();
                $foreignAttributeName = isset($attributeData["attribute"]) ? $attributeData["attribute"] : $foreignSchema->getFirstKey();
                $foreignAttributeData = $foreignSchema->getAttribute($foreignAttributeName);

                if (isset($foreignAttributeData["type"])) {
                    $attributeData["type"] = isset($attributeData["type"]) ? $attributeData["type"] : $foreignAttributeData["type"];
                }

                if (isset($foreignAttributeData["length"])) {
                    $attributeData["length"] = isset($attributeData["length"]) ? $attributeData["length"] : $foreignAttributeData["length"];
                }

                if (isset($foreignAttributeData["unsigned"])) {
                    $attributeData["unsigned"] = isset($attributeData["unsigned"]) ? $attributeData["unsigned"] : $foreignAttributeData["unsigned"];
                }


                $foreignKeys[$attributeName] = array(
                    "table"    => $foreignSchema->getTable(),
                    "column"   => isset($foreignAttributeData["column"]) ? $foreignAttributeData["column"] : $foreignAttributeName,
                    "onDelete" => isset($attributeData["onDelete"]) ? $attributeData["onDelete"] : null,
                    "onUpdate" => isset($attributeData["onUpdate"]) ? $attributeData["onUpdate"] : null
                );

            }


            $schemaObject->attribute($attributeName, $attributeData);
        }

        foreach ($foreignKeys as $attributeName => $foreignKeyData) {
            $schemaObject->foreignKey($attributeName, $foreignKeyData);
        }

        if (isset($array['db'])) {
            $schemaObject->db($array['db']);
        }

        if (isset($array['indexes'])) {
            foreach ($array["indexes"] as $indexName => $indexColumns) {
                $schemaObject->index($indexName, $indexColumns);
            }
        }

        if (isset($array['unique'])) {
            foreach ($array["unique"] as $uniqueAttributes) {
                $schemaObject->unique($uniqueAttributes);
            }
        }

        if (isset($array['triggers'])) {
            foreach ($array["triggers"] as $triggerTable => $operationData) {
                foreach ($operationData as $triggerEvent => $whenData) {
                    foreach ($whenData as $triggerTime => $triggerSQL) {
                        $schemaObject->trigger($triggerTime, $triggerEvent, $triggerTable, $triggerSQL);
                    }
                }
            }
        }

        return $schemaObject;
    }

}
