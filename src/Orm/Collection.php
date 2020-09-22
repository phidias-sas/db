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

    // User defined condition functions
    private $customConditions;

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
        $this->maxPileSize   = 2500;
        $this->insertCount   = 0;

        $this->updateValues  = array();

        $this->customConditions = array();
    }

    public function __destruct()
    {
        $this->save();
    }

    public function getDb()
    {
        return $this->db;
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
        if (!is_scalar($condition)) {
            return $this->whereObject($condition);
        }

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

        // $words = explode(' ', trim($query));
        // No partir por espacios en cadenas entre comillas
        $words = preg_split('/("[^"]*")|\h+/', $query, -1, PREG_SPLIT_NO_EMPTY|PREG_SPLIT_DELIM_CAPTURE);

        foreach ($words as $word) {
            if (!$word = trim($word)) {
                continue;
            }

            if (substr($word,0,1) == '"') {
                $word = substr($word,1,-1);
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

    public function orderBy($attribute, $descending = false, $priorize = false)
    {
        $orderString = $descending ? 'DESC' : 'ASC';
        $parts = explode(".", $attribute);

        // Check for JSON type field
        if ($this->schema->isJson($parts[0])) {
            $attributeName = array_shift($parts);
            $jsonPath = implode(".", $parts);
            return $this->order("JSON_EXTRACT($attributeName, '$.$jsonPath') $orderString", null, $priorize);
        }

        $targetCollection = $this;
        $parts = explode(".", $attribute);
        $attributeName = array_pop($parts);

        foreach ($parts as $part) {
            if (!isset($targetCollection->joins[$part])) {
                trigger_error("orderBy attribute '$attribute' not found", E_USER_WARNING);
                return $this;
            }

            $targetCollection = $this->joins[$part]["collection"];
        }

        if ( !$targetCollection->schema->hasAttribute($attributeName) ) {
            trigger_error("orderBy attribute '$attribute' not found", E_USER_WARNING);
            return $this;
        }

        return $this->order("$attribute $orderString", null, $priorize);
    }

    public function order($order = null, $parameters = null, $priorize = false)
    {
        if ($order === null) {
            $this->orderBy = array();

            return $this;
        }

        $orderString = $parameters ? $this->db->bindParameters($order, $parameters) : $order;

        if ($priorize) {
            array_unshift($this->orderBy, $orderString);
        } else {
            array_push($this->orderBy, $orderString);
        }

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


    /*
    Experimental limit functions for correct limiting
    unique columns in joined sets
    */
    public function limitDistinct($limit, $offset = null)
    {
        $keyName = $this->schema->getKeys()[0];

        $clone = $this->_clone();
        $clone->_removeNonessentialJoins();
        $clone->_clearAttributes();
        $clone->attribute($keyName, "DISTINCT($keyName)");

        $subQuery = $clone->getQuery(null, false);
        $offset ? $subQuery->limit($offset, $limit) : $subQuery->limit($limit);   //DB/Query ->limit() takes argument in the same order as MySQL's LIMIT statement

        $subQuerySQL = $subQuery->toSQL();

        // This is the only way to allow a LIMIT in the subquery.
        // see: http://stackoverflow.com/questions/7124418/mysql-subquery-limit
        return $this->where("$keyName IN (SELECT * FROM ($subQuerySQL) as t)");
    }

    private function _clone()
    {
        $retval = clone($this);
        foreach ($this->joins as $key => $join) {
            $this->joins[$key]["collection"] = $join["collection"]->_clone();
        }
        return $retval;
    }

    private function _clearAttributes()
    {
        $this->attributes = [];
        foreach ($this->joins as $join) {
            $join["collection"]->_clearAttributes();
        }
        return $this;
    }

    // removes all joins not relevant to filtering the query results
    // i.e. joins not relevant to the WHERE, GROUP BY or HAVING clauses of the final query
    private function _removeNonessentialJoins()
    {
        $uses = implode(' ', array_merge($this->where, $this->groupBy, $this->having, $this->orderBy));

        foreach (array_keys($this->joins) as $joinName) {
            if (strpos($uses, $joinName.'.') === false ) {  // this join is not used to filter the query
                unset($this->joins[$joinName]);
            }
        }

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

    public function exclude($collection)
    {
        $this->consolidateConditions();
        $collection->consolidateConditions();

        $newConditions = array();

        if ($this->where) {
            $newConditions[] = '('.implode(' AND ', $this->where).')';
        }

        if ($collection->where) {
            $newConditions[] = 'NOT ('.implode(' AND ', $collection->where).')';
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
            if ($outgoingRelations) {
                $relatedAttribute        = array_keys($outgoingRelations)[0];
                $relatedAttributeIsLocal = true;
            } else {
                $incomingRelations = $collection->schema->getForeignKeys($localTable);
                if ($incomingRelations) {
                    $relatedAttribute        = array_keys($incomingRelations)[0];
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
        // Ignore full strings enclosed between ' OR ` OR "
        $matches = [];
        $enclosures = [];
        preg_match_all('/[\'"`].+?[\'"`]/', $string, $matches);

        if ($matches[0]) {
            foreach ($matches[0] as $k => $enclosed) {
                $enclosures["##-$k-##"] = $enclosed;
            }
            $string = str_replace($enclosures, array_keys($enclosures), $string);
        }

        // Replaces all terms from the alias map
        $patterns     = array();
        $replacements = array();
        foreach ($aliasMap as $source => $target) {
            $patterns[]     = "/([^a-zA-Z0-9_.`']|\A){$source}([^a-zA-Z0-9_.`']|\z)/";
            $replacements[] = "\$1{$target}\$2";
        }
        $retval = preg_replace($patterns, $replacements, $string);

        // Leave enclosures as they were
        if ($enclosures) {
            $retval = str_replace(array_keys($enclosures), $enclosures, $retval);
        }

        return $retval;
    }

    public function getQuery($aliasMap = null, $forceSelectKeys = true)
    {
        if ($aliasMap === null) {
            $aliasMap = $this->buildAliasMap();
        }

        $tableName   = $this->schema->getTable();
        $tableAlias  = $this->alias === null ? $tableName : $this->alias;
        $fieldPrefix = $this->alias === null ? "" : "{$this->alias}.";

        $query      = new Query($tableName, $tableAlias);

        if ($forceSelectKeys) {
            foreach ($this->schema->getKeys() as $keyAttributeName) {
                $query->field($fieldPrefix.$keyAttributeName, "`{$tableAlias}`.".$this->schema->getColumn($keyAttributeName));
            }
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
            $nestedQuery             = $nestedCollection->getQuery($aliasMap, $forceSelectKeys);

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

            // JSON! Add iterator filter to decode JSON values
            if ($this->schema->isJson($attributeName)) {
                $iterator->addFilter(function($obj) use ($attributeName) {
                    $obj->$attributeName = json_decode($obj->$attributeName);
                });
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
            throw new Exception\EntityNotFound($targetKey, "No records for key ".json_encode($targetKey));
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
                if (isset($lastRow[$columnName]) && $lastRow[$columnName] != DB::KEYWORD_DEFAULT) {
                    $entity->$attributeName = $lastRow[$columnName] == DB::KEYWORD_NULL ? null : $lastRow[$columnName];

                    // JSON! decode newly saved value
                    if ($this->schema->isJson($attributeName)) {
                        $entity->$attributeName = json_decode($entity->$attributeName);
                    }

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
        if ($value === null) {

            //see if the collection defined a fixed attribute value via ->set()
            if (isset($this->updateValues[$attributeName]) && is_scalar($this->updateValues[$attributeName])) {
                return $this->updateValues[$attributeName];
            }

            if ( !$this->schema->acceptsNull($attributeName) ) {
                return $this->schema->isAutoIncrement($attributeName) ? null : Db::KEYWORD_DEFAULT;
            }
        }

        // JSON! Encode incoming values for columns of type JSON
        if ($this->schema->isJson($attributeName)) {
            return $value === null ? null : json_encode($value);
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
        return base_convert($uuid, 10, 36) . self::getRandomChar() . self::getRandomChar() . self::getRandomChar();
    }

    private static function getRandomChar()
    {
        $charset = "0123456789abcdefghijklmnopqrstuvwxyz";
        return $charset[\rand(0, 35)];
    }

    public function getUniqueIdTimestamp($uuid)
    {
        return floor( base_convert( substr($uuid, 0, -3), 36, 10) / 10000 ) + 1424445470;

        //for MySQL:
        //FROM_UNIXTIME(FLOOR( CONV(id, 36, 10) / 10000 ) + 1424445470) as timestamp
    }

    public function setCustomConditions($conditions)
    {
        $this->customConditions = $conditions;
        return $this;
    }

    public function whereObject($condition)
    {
        // convert arrays into objects
        if (is_array($condition) && isset($condition["type"])) {
            $conditionObj        = new \stdClass;
            $conditionObj->type  = $condition["type"];
            $conditionObj->model = isset($condition["model"]) ? $condition["model"]: null;

            $condition = $conditionObj;
        }

        if (!isset($condition->type)) {
            throw new \Exception("Invalid condition ".json_encode($condition));
        }

        $hasConditions = false;

        switch ($condition->type) {
            case "or":
                $newCollection = (new Collection($this->schema, $this->db))->setCustomConditions($this->customConditions);
                foreach ($condition->model as $subCondition) {
                    $hasConditions = true;
                    $newCollection->union((new Collection($this->schema, $this->db))->setCustomConditions($this->customConditions)->whereObject($subCondition));
                }
                $hasConditions && $this->intersect($newCollection);
            break;

            case "and":
                foreach ($condition->model as $subCondition) {
                    $hasConditions = true;
                    $this->intersect((new Collection($this->schema, $this->db))->setCustomConditions($this->customConditions)->whereObject($subCondition));
                }
            break;

            case "not":
                $hasConditions = true;
                $this->exclude((new Collection($this->schema, $this->db))->setCustomConditions($this->customConditions)->whereObject($condition->model));
            break;

            case "attributes":
                if (!empty($condition->model)) {
                    $hasConditions = true;
                    $this->match($condition->model);
                }
            break;

            default:
                if (isset($this->customConditions[$condition->type])) {
                    $hasConditions = true;
                    $this->customConditions[$condition->type]($this, $condition->model);
                }
            break;
        }

        if (!$hasConditions) {
            $this->where("0");
        }

        return $this;
    }

    /*
    MongoDb queries
    $mongoCondition = json_decode(<<<STRING
    {
        "&or": [
            {"firstname": {"&like": "ros%"}},
            {
                "&and": [
                    {"firstname": {"&like": "san%"}},
                    {"lastname": {"&like": "cor%"}}
                ]
            },
            {"id": {"&in": [1, 2, 3]}}
        ]
    }
    STRING);

    $people->mongo($mongoCondition);
    */
    public function mongo($condition)
    {
        return $this->where($this->mongoToCondition($condition));
    }

    private function mongoToCondition($condition)
    {
        $keys = array_keys(get_object_vars($condition));
        if (count($keys) !== 1) {
            throw new \Exception("Invalid MongoDB condition ".json_encode($condition));
        }

        if ($keys[0] == "&or" || $keys[0] == "&and") {
            $allConditions = [];
            foreach ($condition->{$keys[0]} as $subcondition) {
                $allConditions[] = "( " . $this->mongoToCondition($subcondition) . " )";
            }

            $glue = $keys[0] == "&or" ? " OR " : " AND ";
            return implode($glue, $allConditions);
        } else {
            $attribute = $keys[0];
            $subcondition = $condition->{$keys[0]};

            /*
            Si el atributo de la condicion tiene puntos
            i.e.
            {"algo.hola": {"&eq": "mundo"}}

            Se asume que el campo "algo" es de tipo JSON y que el condicional aplica a los datos que contiene
            */
            $parts = explode(".", $attribute, 2);
            $attributeName = $parts[0];

            if ($this->schema->isJson($attributeName)) {
                $jsonCondition = new \stdClass;
                $jsonCondition->{$parts[1]} = $subcondition;
                return self::buildJsonCondition($attributeName, $jsonCondition);
            }

            $parts = array_keys(get_object_vars($subcondition));
            $operator = $parts[0];
            $value = $subcondition->$operator;

            $sqlOp = Operator::getSQLOperator($operator);

            return $attribute . " " . $sqlOp . " " . $this->db->sanitizeValue($value);
        }
    }


    /*
    MongoDb queries exclusivamente para campos tipo JSON
    */
    public function whereJson($attributeName, $jsonCondition)
    {
        return $this->where(self::buildJsonCondition($attributeName, $jsonCondition));
    }

    private static function buildJsonCondition($attributeName, $condition)
    {
        if (!$condition || !is_object($condition)) {
            throw new \Exception("Invalid MongoDB Condition");
        }

        $attribute = array_keys(get_object_vars($condition))[0];
        $value = $condition->$attribute;

        if ($attribute == "&or" || $attribute == "&and") {
            $conditions = [];
            foreach ($value as $subcondition) {
                $conditions[] = self::buildJsonCondition($attributeName, $subcondition);
            }
            $glue = $attribute == "&or" ? " OR " : " AND ";
            return "(" . implode($glue, $conditions) . ")";
        }

        $operator = array_keys(get_object_vars($value))[0];
        $argument = $value->$operator;

        if (is_numeric($argument)) {
            $argument = (int)$argument;
        }

        switch ($operator) {
            case '&like':
                return "JSON_SEARCH($attributeName, 'one', '$argument', NULL, '$.$attribute') IS NOT NULL";

            case '&beginsWith':
                return "JSON_SEARCH($attributeName, 'one', '$argument%', NULL, '$.$attribute') IS NOT NULL";

            case '&endsWith':
                return "JSON_SEARCH($attributeName, 'one', '%$argument', NULL, '$.$attribute') IS NOT NULL";

            case '&contains':
                return "JSON_SEARCH($attributeName, 'one', '%$argument%', NULL, '$.$attribute') IS NOT NULL";

            case '&eq':
                $parsedArgument = json_encode($argument);
                return "JSON_CONTAINS($attributeName, '$parsedArgument', '$.$attribute')";

            case '&neq':
                $parsedArgument = json_encode($argument);
                return "NOT JSON_CONTAINS($attributeName, '$parsedArgument', '$.$attribute')";

            case '&gt':
                return "JSON_EXTRACT($attributeName, '$.$attribute') > $argument";

            case '&gte':
                return "JSON_EXTRACT($attributeName, '$.$attribute') >= $argument";

            case '&lt':
                return "JSON_EXTRACT($attributeName, '$.$attribute') < $argument";

            case '&lte':
                return "JSON_EXTRACT($attributeName, '$.$attribute') <= $argument";

            case '&hasAny':
            case '&hasAll':
                $targetValues = (array)$argument;
                $conditions = [];
                foreach ($targetValues as $targetValue) {
                    $parsedValue = json_encode($targetValue);
                    $conditions[] = "JSON_CONTAINS($attributeName, '$parsedValue', '$.$attribute')";
                }

                $glue = $operator == '&hasAny' ? ' OR ' : ' AND ';
                return "(" . implode($glue, $conditions) . ")";
        }
    }

}