<?php namespace Phidias\Db\Orm;

use Phidias\Db\Db;
use Phidias\Db\Query;
use Phidias\Db\Iterator;
use Phidias\Db\Table\Schema;

class Collection
{
    private $schema;
    private $alias;
    private $className;

    private $attributes;
    private $joins;         //joined (or "nested") collections

    private $where;
    private $orderBy;
    private $groupBy;
    private $having;

    private $limit;
    private $offset;
    private $page;

    //Helper attributes set when this collection is being joined
    //see notEmpty() and relatedWith()
    private $joinAsInner;
    private $relatedAttribute;

    //Db connection
    private $db;

    //behaviour
    private $hasOneElement;
    private $iterator;  //user-defined iterator
    private $filters;   //iterator filters

    //Unit of work
    private $pile;
    private $maxPileSize;
    private $insertCount;

    private $updateValues;

    private static $uniqueSequence = 0;

    public static function create($schema)
    {
        return new Collection($schema);
    }

    public static function load($db, $tableName)
    {
        return new Collection(Schema::load($db, $tableName), $db);
    }

    public function __construct($schema, $db = null)
    {
        $this->schema        = $schema;
        $this->alias         = null;
        $this->className     = "\stdClass";

        $this->attributes    = array();
        $this->joins         = array();

        $this->where         = array();
        $this->orderBy       = array();
        $this->groupBy       = array();
        $this->having        = array();

        $this->limit         = null;
        $this->offset        = null;
        $this->page          = null;

        $this->db            = $db === null ? Db::connect($this->schema->getDb()) : $db;

        $this->hasOneElement = false;
        $this->iterator      = null;
        $this->filters       = array();

        $this->pile          = array();
        $this->maxPileSize   = 5000;
        $this->insertCount   = 0;

        $this->updateValues  = array();
    }

    public function __destruct()
    {
        $this->save();
    }

    public function alias($alias)
    {
        $this->alias = $alias;

        foreach ($this->joins as $joinName => $join) {
            $join["collection"]->alias("$alias.$joinName");
        }

        return $this;
    }

    public function getAlias()
    {
        return $this->alias === null ? $this->schema->getTable() : $this->alias;
    }

    public function className($className)
    {
        $this->className = $className;

        return $this;
    }

    public function hasOneElement($bool = true)
    {
        $this->hasOneElement = $bool;

        return $this;
    }

    public function iterator($iterator)
    {
        $this->iterator = $iterator;

        return $this;
    }

    public function addFilter($filter)
    {
        $this->filters[] = $filter;

        return $this;
    }

    public function attribute($attributeName, $origin = null, $joinCondition = null)
    {
        if ($origin === null && !$this->schema->hasAttribute($attributeName)) {
            trigger_error("unknown attribute '$attributeName'", E_USER_WARNING);

            return $this;
        }

        if ($origin instanceof Collection) {
            $this->join($attributeName, $origin, $joinCondition);
            $this->attributes[$attributeName] = null;
        } else {
            $this->attributes[$attributeName] = $origin;
        }

        return $this;
    }

    public function attributes($attributeArray = null)
    {
        if (!is_array($attributeArray)) {
            $attributeArray = func_get_args();
        }

        foreach ($attributeArray as $attr) {
            $this->attribute($attr);
        }

        return $this;
    }

    public function allAttributes()
    {
        foreach (array_keys($this->schema->getAttributes()) as $attributeName) {
            $this->attribute($attributeName);
        }

        return $this;
    }

    public function getAttributes()
    {
        return $this->attributes;
    }

    //Query filtering functions

    public function where($condition, $parameters = null)
    {
        $this->where[] = $parameters ? $this->db->bindParameters($condition, $parameters) : $condition;

        return $this;
    }

    public function match($attributeName, $value = null, $mongoOperator = '&eq')
    {
        if (is_object($attributeName)) {
            return $this->matchObject($attributeName);
        }

        if ($value === null) {

            $this->where("$attributeName IS null");

        } elseif (is_scalar($value)) {

            $queryOperator = Operator::getSQLOperator($mongoOperator);
            $this->where("$attributeName $queryOperator :value", array('value' => $value));

        } elseif (is_array($value)) {

            $targetArray = $this->normalizeArray($value);

            if ($targetArray) {

                if ($mongoOperator === "&between") {
                    $this->where("$attributeName BETWEEN :low AND :high", array('low' => $targetArray[0], 'high' => $targetArray[1]));
                } else {
                    $operator = $mongoOperator == '&nin' ? 'NOT IN' : 'IN';
                    $this->where("$attributeName $operator :value", array('value' => $targetArray));
                }

            }

        } elseif ($value instanceof Collection) {
            return $this->inCollection($attributeName, $value);
        }

        return $this;
    }

    private function normalizeArray($array)
    {
        $targetArray = array();

        foreach ($array as $element) {
            if (is_object($element)) {
                $firstKey = $this->schema->getKeys()[0];
                if (isset($element->$firstKey)) {
                    $targetArray[] = $element->$firstKey;
                }
            } elseif (is_scalar($element)) {
                $targetArray[] = $element;
            }
        }

        return $targetArray;
    }

    private function inCollection($attributeName, $collection)
    {
        $singleAttribute = $collection->relatedAttribute ? $collection->relatedAttribute : $collection->schema->getKeys()[0];

        $nestedSelect = $collection->getQuery()
                                   ->mergeJoined()
                                   ->fields($singleAttribute)
                                   ->limit(null)
                                   ->toSQL();

        return $this->where("$attributeName IN ($nestedSelect)");
    }

    private function matchObject($object)
    {
        foreach ($object as $attributeName => $value) {

            if (isset($this->joins[$attributeName]) && is_object($value)) {

                $this->joins[$attributeName]["collection"]->matchObject($value);

            } elseif ($this->schema->hasAttribute($attributeName)) {

                $fullAttribute = $this->alias === null ? $attributeName : "$this->alias.$attributeName";

                if (Operator::isOperator($value)) {
                    $this->match($fullAttribute, Operator::getValue($value), Operator::getOperator($value));
                } else {
                    $this->match($fullAttribute, $value);
                }

            }
        }

        return $this;
    }

    public function search($query, $attributes, $matchAllWords = true)
    {
        if (!trim($query)) {
            return $this;
        }

        $attributes = (array) $attributes;

        if (!count($attributes)) {
            trigger_error("no searchable attributes specified", E_USER_WARNING);

            return $this;
        }

        $wordConditions = array();

        $words = explode(' ', trim($query));
        foreach ($words as $word) {
            if (!$word = trim($word)) {
                continue;
            }
            $word = str_replace('%', '\%', $word);

            $matchingConditions = array();
            foreach ($attributes as $attributeName) {
                $matchingConditions[] = "$attributeName LIKE :word";
            }

            $matchingCondition = '(' . implode(' OR ', $matchingConditions) . ')';
            $wordConditions[]  = $this->db->bindParameters($matchingCondition, array('word' => "%$word%"));
        }

        $finalCondition = $matchAllWords ? implode(' AND ', $wordConditions) : implode(' OR ', $wordConditions);
        $this->where($finalCondition);

        return $this;
    }

    //Query ordering functions

    public function orderBy($attribute, $descending = false)
    {
        $targetCollection = $this;
        $parts            = explode(".", $attribute);
        $attributeName    = array_pop($parts);

        foreach ($parts as $part) {

            if (!isset($targetCollection->joins[$part])) {
                trigger_error("orderBy attribute '$attribute' not found", E_USER_WARNING);
                return $this;
            }

            $targetCollection = $this->joins[$part]["collection"];

        }

        if (!array_key_exists($attributeName, $targetCollection->attributes)) {
            trigger_error("orderBy attribute '$attribute' not found", E_USER_WARNING);
            return $this;
        }

        $sortString = $descending ? 'DESC' : 'ASC';

        return $this->order("$attribute $sortString");
    }

    public function order($order = null, $parameters = null)
    {
        if ($order === null) {
            $this->orderBy = array();

            return $this;
        }

        $this->orderBy[] = $parameters ? $this->db->bindParameters($order, $parameters) : $order;

        return $this;
    }

    public function groupBy($group, $parameters = null)
    {
        $this->groupBy[] = $parameters ? $this->db->bindParameters($group, $parameters) : $group;

        return $this;
    }

    public function having($condition, $parameters = null)
    {
        $this->having[] = $parameters ? $this->db->bindParameters($condition, $parameters) : $condition;

        return $this;
    }

    //built in paging
    public function limit($limit)
    {
        $this->limit = $limit !== null ? max(1, (int) $limit) : null;

        return $this;
    }

    public function getLimit()
    {
        return $this->limit;
    }

    public function offset($offset)
    {
        $this->offset = max(0, (int) $offset);
        $this->page   = $this->limit === null ? 1 : ( 1 + floor($this->offset / $this->limit) );

        return $this;
    }

    public function getOffset()
    {
        return $this->offset;
    }

    public function page($page)
    {
        $this->page   = max(1, (int) $page);
        $this->offset = $this->limit === null ? 0 : $this->limit * ($this->page - 1);

        return $this;
    }

    public function getPage()
    {
        return $this->page === null ? 1 : $this->page;
    }

    // Unions and intersections with other collections
    public function union($collection)
    {
        $this->consolidateConditions();
        $collection->consolidateConditions();

        $newConditions = array();

        if ($this->where) {
            $newConditions[] = '('.implode(' AND ', $this->where).')';
        }

        if ($collection->where) {
            $newConditions[] = '('.implode(' AND ', $collection->where).')';
        }

        if ($newConditions) {
            $this->where = array('('.implode(' OR ', $newConditions).')');
        }

        return $this;
    }

    public function intersect($collection)
    {
        $this->consolidateConditions();
        $collection->consolidateConditions();

        $newConditions = array();

        if ($this->where) {
            $newConditions[] = '('.implode(' AND ', $this->where).')';
        }

        if ($collection->where) {
            $newConditions[] = '('.implode(' AND ', $collection->where).')';
        }

        $this->where = $newConditions;

        return $this;
    }

    private function consolidateConditions()
    {
        foreach ($this->joins as $joinData) {
            $joinData['collection']->consolidateConditions();
            $this->where = array_merge($this->where, $joinData['collection']->where);
            $joinData['collection']->where = array();
        }
    }

    //Functions for joining (nesting) collections

    public function notEmpty()
    {
        $this->joinAsInner = true;

        return $this;
    }

    public function relatedWith($attribute)
    {
        $this->relatedAttribute = $attribute;

        return $this;
    }

    public function join($attributeName, $collection, $joinCondition = null)
    {
        $collection->alias($this->alias === null ? $attributeName : "$this->alias.$attributeName");

        if ($joinCondition !== null) {

            $this->joins[$attributeName] = array(
                "collection"    => $collection,
                "joinCondition" => $joinCondition
            );

            return $this;
        }


        //Attempt to deduce the relation with the incoming collection
        $relatedAttribute        = null;
        $relatedAttributeIsLocal = null;

        //when the specified attribute name is a foreign key
        if ($this->schema->hasForeignKey($attributeName)) {
            $relatedAttribute        = $attributeName;
            $relatedAttributeIsLocal = true;
        }

        //when the incoming collection defined a related attribute
        if ($relatedAttribute === null && $collection->relatedAttribute !== null) {

            if ($this->schema->hasForeignKey($collection->relatedAttribute)) {

                $relatedAttribute        = $collection->relatedAttribute;
                $relatedAttributeIsLocal = true;

            } elseif ($collection->schema->hasForeignKey($collection->relatedAttribute)) {

                $relatedAttribute        = $collection->relatedAttribute;
                $relatedAttributeIsLocal = false;

            } elseif ($collection->schema->hasAttribute($collection->relatedAttribute)) { //last resort: the foreign schema has an attribute with the given name

                $relatedAttribute        = $collection->relatedAttribute;
                $relatedAttributeIsLocal = false;

            } else {
                trigger_error("foreign attribute '{$collection->relatedAttribute}' not found", E_USER_ERROR);
            }

        }

        //when there is a single relation between the tables

        $localTable   = $this->schema->getTable();
        $foreignTable = $collection->schema->getTable();

        if ($relatedAttribute === null) {

            $outgoingRelations = $this->schema->getForeignKeys($foreignTable);
            if (count($outgoingRelations) == 1) {
                $relationNames           = array_keys($outgoingRelations);
                $relatedAttribute        = array_pop($relationNames);
                $relatedAttributeIsLocal = true;
            } else {
                $incomingRelations = $collection->schema->getForeignKeys($localTable);
                if (count($incomingRelations) == 1) {
                    $relationNames           = array_keys($incomingRelations);
                    $relatedAttribute        = array_pop($relationNames);
                    $relatedAttributeIsLocal = false;
                }
            }

        }

        //no relation could be determined
        if ($relatedAttribute === null) {
            trigger_error("could not determine relation between tables '$localTable' and '$foreignTable'", E_USER_ERROR);
        }

        //get the relation data and register the join
        $relationData = $relatedAttributeIsLocal ? $this->schema->getForeignKey($relatedAttribute) : $collection->schema->getForeignKey($relatedAttribute);
        if ($relationData === null) {

            if ($attributeData = $collection->schema->getAttribute($relatedAttribute)) {
                $relationData["column"] = $this->schema->getKeys()[0];
            } else {
                trigger_error("foreign attribute '$relatedAttribute' not found", E_USER_ERROR);
            }

        }

        $this->joins[$attributeName] = array(
            "collection"    => $collection,
            "localColumn"   => $relatedAttributeIsLocal ? $this->schema->getColumn($relatedAttribute) : $relationData["column"],
            "foreignColumn" => $relatedAttributeIsLocal ? $relationData["column"] : $collection->schema->getColumn($relatedAttribute)
        );

        return $this;
    }

    private function buildAliasMap()
    {
        $tableAlias  = $this->alias === null ? $this->schema->getTable() : $this->alias;
        $fieldPrefix = $this->alias === null ? "" : "{$this->alias}.";

        $retval     = array();

        foreach ($this->schema->getAttributes() as $attributeName => $attributeData) {
            $retval[$fieldPrefix.$attributeName] = '`'.$tableAlias.'`.`'.$attributeData['column'].'`';
        }

        foreach ($this->joins as $name => $joinData) {
            $retval = array_merge($retval, $joinData["collection"]->buildAliasMap());
        }

        //Custom attributes
        foreach ($this->attributes as $attributeName => $attributeSource) {
            if ($attributeSource !== null && !$this->schema->hasAttribute($attributeName)) {
                $retval[$fieldPrefix.$attributeName] = '('.$this->translate($attributeSource, $retval).')';
            }
        }

        return $retval;
    }

    private function translate($string, $aliasMap)
    {
        $patterns     = array();
        $replacements = array();

        foreach ($aliasMap as $source => $target) {
            $patterns[]     = "/([^a-zA-Z0-9_.`']|\A){$source}([^a-zA-Z0-9_.`']|\z)/";
            $replacements[] = "\$1{$target}\$2";
        }

        return preg_replace($patterns, $replacements, $string);
    }

    public function getQuery($aliasMap = null)
    {
        if ($aliasMap === null) {
            $aliasMap = $this->buildAliasMap();
        }

        $tableName   = $this->schema->getTable();
        $tableAlias  = $this->alias === null ? $tableName : $this->alias;
        $fieldPrefix = $this->alias === null ? "" : "{$this->alias}.";

        $query      = new Query($tableName, $tableAlias);

        //Always select keys
        foreach ($this->schema->getKeys() as $keyAttributeName) {
            $query->field($fieldPrefix.$keyAttributeName, "`{$tableAlias}`.".$this->schema->getColumn($keyAttributeName));
        }

        //Select working attributes
        foreach ($this->attributes as $attributeName => $attributeOrigin) {

            if (isset($this->joins[$attributeName])) {
                continue;
            }

            if ($attributeOrigin === null) {
                $attributeOrigin = "`{$tableAlias}`.".$this->schema->getColumn($attributeName);
            } else {
                $attributeOrigin = $this->translate($attributeOrigin, $aliasMap);
            }

            $query->field($fieldPrefix.$attributeName, $attributeOrigin);
        }

        //Join with nested collections
        foreach ($this->joins as $attributeName => $joinData) {

            $joinType = $joinData["collection"]->joinAsInner ? "INNER" : "LEFT";

            if (isset($joinData["joinCondition"])) {
                $conditions = array($this->translate($joinData["joinCondition"], $aliasMap));
            } else {

                $localColumn       = $joinData["localColumn"];
                $foreignTableAlias = $joinData["collection"]->alias;
                $foreignColumn     = $joinData["foreignColumn"];

                $conditions  = array("`$tableAlias`.`$localColumn` = `$foreignTableAlias`.`$foreignColumn`");
            }

            foreach ($joinData["collection"]->where as $condition) {
                $conditions[] = $this->translate($condition, $aliasMap);
            }

            $nestedCollection        = clone($joinData["collection"]);
            $nestedCollection->where = array();
            $nestedQuery             = $nestedCollection->getQuery($aliasMap);

            $query->join($joinType, $nestedQuery, $conditions);
        }

        //conditionals, grouping and ordering
        foreach ($this->where as $condition) {
            $query->where($this->translate($condition, $aliasMap));
        }

        foreach ($this->orderBy as $order) {
            $query->orderBy($this->translate($order, $aliasMap));
        }

        foreach ($this->groupBy as $group) {
            $query->groupBy($this->translate($group, $aliasMap));
        }

        foreach ($this->having as $condition) {
            $query->having($this->translate($condition, $aliasMap));
        }

        //paging
        if ($this->limit !== null) {
            if ($this->offset !== null) {
                $query->limit($this->offset, $this->limit);
            } else {
                $query->limit($this->limit);
            }
        }

        return $query;
    }

    /**
     * Build an iterator for the resultSet generated from this->getQuery
     */
    private function buildIterator()
    {
        $fieldPrefix = $this->alias === null ? "" : "{$this->alias}.";

        $keyFields = array();
        foreach ($this->schema->getKeys() as $attributeName) {
            $keyFields[] = $fieldPrefix.$attributeName;
        }

        $iterator = new Iterator($keyFields, $this->className, $this->hasOneElement);

        foreach (array_keys($this->attributes) as $attributeName) {
            if (isset($this->joins[$attributeName])) {
                $iterator->attribute($attributeName, $this->joins[$attributeName]["collection"]->buildIterator());
            } else {
                $iterator->attribute($attributeName, $fieldPrefix.$attributeName);
            }
        }

        foreach ($this->filters as $filter) {
            $iterator->addFilter($filter);
        }

        return $iterator;
    }

    public function find($targetKey = null)
    {
        if ($targetKey !== null) {
            return $this->fetch($targetKey);
        }

        $query       = $this->getQuery()->toSQL();
        $resultSet   = $this->db->query($query);
        $iterator    = $this->iterator === null ? $this->buildIterator() : $this->iterator;

        $iterator->setResultSet($resultSet);

        return $iterator;
    }

    public function fetch($targetKey = null)
    {

        if ($targetKey !== null) {

            $targetKey = (array) $targetKey;
            foreach ($this->schema->getKeys() as $index => $keyAttributeName) {
                if (isset($targetKey[$index])) {
                    $this->match($keyAttributeName, $targetKey[$index]);
                }
            }

        }

        $result = $this->find()->first();

        if ($result === null) {
            throw new Exception\EntityNotFound($targetKey);
        }

        return $result;
    }

    public function first()
    {
        return $this->limit(1)->find()->first();
    }

    public function count()
    {
        $countQuery = $this->getQuery();
        $countQuery->limit(null);

        $countSQL  = $countQuery->toSQL();
        $resultSet = $this->db->query("SELECT COUNT(*) as `count` FROM ($countSQL) countTable");
        $retval    = $resultSet->fetch_assoc();

        return isset($retval['count']) ? (int) $retval['count'] : null;
    }

    //Unit of work functions
    public function add($object)
    {
        $this->pile[] = $this->toRow($object);

        if (count($this->pile) >= $this->maxPileSize) {
            $this->save();
        }

        return $this;
    }

    public function clear()
    {
        $this->pile = array();
    }

    public function save(&$entity = null)
    {
        if ($entity !== null) {
            $this->add($entity);
        }

        if (!count($this->pile)) {
            return $this->insertCount;
        }

        $columnNames = array_keys($this->pile[0]);

        $this->insertCount += $this->db->insertUpdate($this->schema->getTable(), $columnNames, $this->pile, $this->schema->getAutoIncrementColumns());

        if ($entity !== null) {

            $lastRow = end($this->pile);

            foreach ($this->schema->getAttributes() as $attributeName => $attributeData) {
                $columnName = $attributeData["column"];
                if (isset($lastRow[$columnName])) {
                    $entity->$attributeName = $lastRow[$columnName];
                } elseif ($this->schema->isAutoIncrement($attributeName)) {
                    $entity->$attributeName = $this->getLastInsertID();
                }
            }

        }

        $this->clear();

        return $this->insertCount;
    }



    /* Return an insertable row (array with column names as key) representing the given object */
    private function toRow($object)
    {
        $row = array();

        /* First, make sure all keys are present */
        foreach ($this->schema->getKeys() as $keyName) {

            $keyValue = isset($object->$keyName) ? $object->$keyName : null;

            if ($keyValue === null) {
                $keyAttribute = $this->schema->getAttribute($keyName);
                if (isset($keyAttribute["uuid"])) {
                    $keyValue = $this->getUniqueId();
                }
            }

            $columnName       = $this->schema->getColumn($keyName);
            $row[$columnName] = $this->sanitizeAttributeValue($keyValue, $keyName);

        }

        /* Now add keys for active collection attributes */
        foreach ($this->attributes as $attributeName => $attributeSource) {


            if ( $this->schema->isKey($attributeName) || !($columnName = $this->schema->getColumn($attributeName)) ) {
                continue;
            }

            $attributeValue   = isset($object->$attributeName) ? $object->$attributeName : $attributeSource;
            $row[$columnName] = $this->sanitizeAttributeValue($attributeValue, $attributeName);

        }

        return $row;
    }


    private function sanitizeAttributeValue($value, $attributeName)
    {
        //Null values on non-null columns
        if ($value === null && !$this->schema->acceptsNull($attributeName)) {

            //see if the collection defined a fixed attribute value via ->set()
            if (isset($this->updateValues[$attributeName]) && is_scalar($this->updateValues[$attributeName])) {
                return $this->updateValues[$attributeName];
            }

            return $this->schema->isAutoIncrement($attributeName) ? null : Db::KEYWORD_DEFAULT;
        }

        if (is_scalar($value)) {
            return $value;
        }

        if (is_object($value) && isset($this->joins[$attributeName])) {

            $expectedKeys = $this->joins[$attributeName]["collection"]->schema->getKeys();

            foreach ($expectedKeys as $keyName) {
                if (isset($value->$keyName)) {
                    return $value->$keyName;
                }
            }

            //Non of the foreign primary keys had values.  Attempt to save
            $value = $this->joins[$attributeName]["collection"]->save($value);

            foreach ($expectedKeys as $keyName) {
                if (isset($value->$keyName)) {
                    return $value->$keyName;
                }
            }

        }

        return Db::KEYWORD_DEFAULT;
    }

    public function set($attributeName, $attributeValue)
    {
        $this->updateValues[$attributeName] = $attributeValue;

        return $this;
    }

    public function update()
    {
        if (!$this->updateValues) {
            trigger_error("no values to update", E_USER_WARNING);
            return 0;
        }

        if (!$this->where) {
            trigger_error("attempt to update ignored because no conditions are defined.  If you wish to update the entire collection use the conditional where(1)", E_USER_WARNING);
            return 0;
        }

        $aliasMap = $this->buildAliasMap();

        $targetValues = array();
        foreach ($this->updateValues as $attributeName => $targetValue) {
            $targetValues[$this->translate($attributeName, $aliasMap)] = $targetValue;
        }

        $updateConditions = array();
        foreach ($this->where as $where) {
            $updateConditions[] = $this->translate($where, $aliasMap);
        }


        $tableName  = $this->schema->getTable();
        $tableAlias = $this->alias === NULL ? $tableName : $this->alias;
        $table      = $tableName.' `'.$tableAlias.'`';

        foreach ($this->joins as $name => $join) {

            $joinTable = $join['collection']->schema->getTable();
            $joinAlias = $join['collection']->alias;

            $joinConditions = array("`$tableAlias`.`{$join['localColumn']}` = `$joinAlias`.`{$join['foreignColumn']}`");
            foreach ($join['collection']->where as $condition) {
                $joinConditions[] = $this->translate($condition, $aliasMap);
            }

            $table .= " JOIN $joinTable `$joinAlias` ON ".implode(" AND ", $joinConditions);
        }

        return $this->db->update($table, $targetValues, implode(' AND ', $updateConditions));
    }

    public function delete()
    {
        if (!$this->where) {
            trigger_error("attempt to delete ignored because no conditions are defined.  If you wish to delete the entire collection use the conditional where(1)", E_USER_WARNING);
            return 0;
        }

        $aliasMap         = $this->buildAliasMap();
        $deleteConditions = array();
        foreach ($this->where as $where) {
            $deleteConditions[] = $this->translate($where, $aliasMap);
        }

        /* Since MySQL does not support "DELETE FROM table a WHERE a.some = thing" */
        $deleteCondition = str_replace("`$this->alias`.", '', implode(' AND ', $deleteConditions));

        return $this->db->delete($this->schema->getTable(), $deleteCondition);
    }

    public function getLastInsertID()
    {
        return $this->db->getLastInsertID();
    }

    public function getNextInsertID()
    {
        return $this->db->getNextInsertID($this->schema->getTable());
    }


    public function getUniqueId()
    {
        $uuid = round( microtime(true) * 10000 ) - 14244454700000 + self::$uniqueSequence++;

        return base_convert($uuid, 10, 36);
    }

    public function getUniqueIdTimestamp($uuid)
    {
        return floor( base_convert($uuid, 36, 10) / 10000 ) + 1424445470;

        //for MySQL:
        //FROM_UNIXTIME(FLOOR( CONV(id, 36, 10) / 10000 ) + 1424445470) as timestamp
    }

}