<?php namespace Phidias\Db;

/**
 *
 * A Query (SELECT statement) builder
 *
 * $select = new Query('people');   //shorthand:  $select = Query::select("people");
 * $select->field('id');
 * $select->field('firstName', 'first_name');
 * $select->field('lastName');
 * $select->field('fullName', 'CONCAT(people.firstName, " ", people.lastName)');
 *
 * SELECT id, first_name as firstName, lastName, CONCAT(people.firstName, " ", people.lastName) as fullName FROM people `people`
 *
 */
class Query
{
    private $tableName;
    private $tableAlias;
    private $fields;
    private $conditions;
    private $joins;
    private $joinData;
    private $orderBy;
    private $limit;
    private $groupBy;
    private $having;
    private $useIndex;

    public static function select($tableName, $tableAlias = null)
    {
        return new Query($tableName, $tableAlias);
    }

    public function __construct($tableName, $tableAlias = null)
    {
        $this->tableName  = $tableName;
        $this->tableAlias = $tableAlias !== null ? $tableAlias : $this->tableName;

        $this->useIndex   = array();
        $this->fields     = array();
        $this->conditions = array();
        $this->joins      = array();
        $this->joinData   = array();
        $this->orderBy    = array();
        $this->groupBy    = array();
    }

    public function fields($fieldArray = null)
    {
        if ($fieldArray === null) {
            $this->fields = array();
            return $this;
        }

        $this->fields = array();

        foreach ((array)$fieldArray as $key => $fieldName) {

            if (is_numeric($key)) {
                $this->field($fieldName);
            } else { //fieldName => origin
                $this->field($key, $fieldName);
            }

        }

        return $this;
    }

    public function field($fieldName, $origin = null)
    {
        if ($origin === null) {
            $origin = $this->tableAlias.'.'.$fieldName;
        }

        $this->fields[$fieldName] = $origin;

        return $this;
    }

    public function where($condition)
    {
        $this->conditions[] = $condition;

        return $this;
    }

    public function limit($offset, $value = null)
    {
        if ($value !== null) {
            $this->limit = "$offset, $value";
        } else {
            $this->limit = $offset;
        }

        return $this;
    }

    public function having($value)
    {
        $this->having = $value;

        return $this;
    }

    public function orderBy($value = null)
    {
        if ($value === null) {
            $this->orderBy = array();
        } else {
            $this->orderBy[] = $value;
        }

        return $this;
    }

    public function groupBy($value = null)
    {
        if ($value === null) {
            $this->groupBy = array();
        } else {
            $this->groupBy[] = $value;
        }

        return $this;
    }

    /**
     * Join this query with another query object
     *
     */
    public function join($type, Query $query, $conditions)
    {
        $this->joins[] = array(
            'type'       => $type,
            'query'      => $query,
            'conditions' => (array) $conditions
        );

        return $this;
    }

    public function useIndex($index)
    {
        $this->useIndex[] = $index;

        return $this;
    }

    public function getAlias()
    {
        return $this->tableAlias;
    }

    public function mergeJoined()
    {
        $retval = clone($this);

        foreach ($retval->joins as $nestedData) {

            $nestedSelect = $nestedData['query']->mergeJoined();

            $retval->joinData[] = array(
                'type'          => strtoupper($nestedData['type']),
                'foreignTable'  => $nestedSelect->tableName,
                'foreignAlias'  => $nestedSelect->tableAlias,
                'joinCondition' => implode(' AND ', $nestedData['conditions'])
            );

            $retval->fields       = array_merge($retval->fields, $nestedSelect->fields);
            $retval->joinData     = array_merge($retval->joinData, $nestedSelect->joinData);
            $retval->conditions   = array_merge($retval->conditions, $nestedSelect->conditions);
        }

        $retval->joins = array();

        return $retval;
    }

    public function toSQL()
    {
        $select = $this->mergeJoined();

        if (!$select->fields) {
            trigger_error("no fields selected for query", E_USER_ERROR);
        }

        $sqlQuery = "SELECT "."\n";
        $allColumns = array();
        foreach ($select->fields as $columnAlias => $columnSource) {
            $allColumns[] = $columnSource.' as `'.$columnAlias.'`';
        }
        $sqlQuery .= implode(', '."\n", $allColumns)." \n";

        $sqlQuery .= "FROM "."\n";
        $sqlQuery .= $select->tableName.' `'.$select->tableAlias."`\n";

        if ($select->useIndex) {
            $sqlQuery .= "USE INDEX(".implode(', ', $select->useIndex).") "."\n";
        }

        foreach ($select->joinData as $joinData) {
            $sqlQuery .= $joinData['type'].' JOIN '.$joinData['foreignTable'].' `'.$joinData['foreignAlias'].'` ON '.$joinData['joinCondition']." \n";
        }

        if ($select->conditions) {
            $sqlQuery .= 'WHERE ('.implode(') AND (', $select->conditions).") \n";
        }

        if ($select->groupBy) {
            $sqlQuery .= "GROUP BY ".implode(', ', $select->groupBy)." \n";
        }

        if ($select->having !== null) {
            $sqlQuery .= "HAVING $select->having"." \n";
        }

        if ($select->orderBy) {
            $orderBy = array();
            foreach ($select->orderBy as $fieldName) {
                $orderBy[] = isset($select->fields[$fieldName]) ? $select->fields[$fieldName] : $fieldName;
            }

            $sqlQuery .= "ORDER BY ".implode(', ', $orderBy)." \n";
        }

        if ($select->limit !== null) {
            $sqlQuery .= "LIMIT $select->limit"." \n";
        }

        return $sqlQuery;
    }

}
