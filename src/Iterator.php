<?php namespace Phidias\Db;

/**
 * Phidias ResultSet iterator
 *
 * This iterator will traverse a mysqli resultSet as if it were a nested object structure.
 *
 * Suppose the following resultSet obtained from an INNER JOIN
 *
 * person.id    person.name     pet.id    pet.owner_id      pet.name
 * ---------------------------------------------------------------------------------------
 * 1            Santiago        1         1                 Rufus
 * 1            Santiago        2         1                 Buddy
 * 1            Santiago        3         1                 Shep
 * 2            Peter           4         2                 Hugo
 * 2            Peter           5         2                 Paco
 * 3            Roger           6         3                 Timmy
 *
 * the idea is to iterate it as:
 *
 * foreach ($person in $iterator) {
 *
 *     echo "$person->name has this pets:";
 *
 *     foreach ($person->pets as $pet) {
 *         echo "$pet->name \n";
 *     }
 * }
 *
 *
 * The top most iterator must jump from person to person, creating an object for each distinct record.
 * To do this, the iterator must know what the key column identifying people is, a className to instantiate the returned object as, and
 * which attributes to populate this object with.
 *
 * $iterator = new Iterator("person.id");
 * $iterator->attribute("name", "person.name");
 * $iterator->attribute("pets", new Iterator("pet.id"));
 *
 * foreach ($iterator as $person) {
 *   echo $person->name;
 * }
 *
 * The iterator will skip records with the same key and advance only when a new key is found.
 *
 *
 */
class Iterator implements \Iterator
{
    private $resultSet;
    private $key;

    private $className;
    private $attributes;
    private $nestedIterators;

    private $lastSeenKeys;
    private $pointer;
    private $currentRow;

    private $assertions;
    private $pointerStart;

    private $fetchFirstRow;
    private $filters;

    public function __construct($keyFields, $className = "\stdClass", $fetchFirstRow = false)
    {
        $this->resultSet       = null;
        $this->key             = (array) $keyFields;

        $this->className       = $className;
        $this->attributes      = array();
        $this->nestedIterators = array();

        $this->lastSeenKeys    = array();
        $this->pointer         = null;
        $this->currentRow      = null;

        $this->assertions      = null;
        $this->pointerStart    = 0;

        $this->fetchFirstRow   = $fetchFirstRow;
        $this->filters         = array();
    }

    public function setResultSet($resultSet)
    {
        $this->resultSet = $resultSet;
        foreach ($this->nestedIterators as $nestedIterator) {
            $nestedIterator->setResultSet($resultSet);
        }

        return $this;
    }

    public function attribute($attributeName, $origin = null)
    {
        if ($origin instanceof Iterator) {
            $this->nestedIterators[$attributeName] = $origin;
        } elseif ($origin === null) {
            $this->attributes[$attributeName] = $attributeName;
        } else {
            $this->attributes[$attributeName] = $origin;
        }

        return $this;
    }

    public function addFilter($function)
    {
        if (!is_callable($function)) {
            trigger_error("filter is not callable", E_USER_ERROR);
        }

        $this->filters[] = $function;

        return $this;
    }

    public function rewind()
    {
        $this->resultSet->data_seek($this->pointerStart);
        $this->pointer     = $this->pointerStart;
        $this->currentRow  = $this->resultSet->fetch_assoc();

        if ($this->currentRow == null) {
            $this->lastSeenKeys = array();
            return;
        }

        foreach ($this->key as $index => $keyField) {
            $this->lastSeenKeys[$index] = $this->currentRow[$keyField];
        }
    }

    public function valid()
    {
        // no row preset
        if ($this->currentRow === null) {
            return false;
        }

        // current key is null
        // this happens when we are in a nested iterator via LEFT join and there are no joined records
        foreach ($this->key as $keyField) {
            if ($this->currentRow[$keyField] === null) {
                return false;
            }
        }

        // All assertions validate
        if ($this->assertions === null) {
            return true;
        }

        foreach ($this->assertions as $fieldName => $expectedValue) {
            if ($this->currentRow[$fieldName] != $expectedValue) {
                return false;
            }
        }

        return true;
    }

    public function key()
    {
        return $this->pointer;
    }

    public function current()
    {
        //create the new object
        $className      = $this->className;
        $returnObject   = new $className;

        $expectedAttributes = [];

        foreach ($this->attributes as $attributeName => $sourceField) {
            $returnObject->$attributeName = is_string($sourceField) && isset($this->currentRow[$sourceField]) ? $this->currentRow[$sourceField] : null;
            $expectedAttributes[$attributeName] = $attributeName;
        }

        //Set nested iterators
        $assertions = array();
        foreach ($this->key as $keyFieldName) {
            $assertions[$keyFieldName]  = $this->currentRow[$keyFieldName];
        }

        foreach ($this->nestedIterators as $attributeName => $nestedIterator) {

            $nestedIterator->assertions   = $assertions;
            $nestedIterator->pointerStart = $this->pointer;

            $returnObject->$attributeName = $nestedIterator->fetchFirstRow ? $nestedIterator->first() : $nestedIterator;

            $expectedAttributes[$attributeName] = $attributeName;
        }

        // Unset all object properties not specified as iterator attributes
        foreach ($returnObject as $attr => $value) {
            if (!isset($expectedAttributes[$attr])) {
                unset($returnObject->$attr);
            }
        }

        // Apply filters
        foreach ($this->filters as $filter) {
            call_user_func_array($filter, array($returnObject));
        }

        return $returnObject;
    }

    public function next()
    {
        //move forward until we see a row with different keys
        while ($this->currentRow !== null && $this->alreadySeen()) {
            $this->resultSet->data_seek(++$this->pointer);
            $this->currentRow = $this->resultSet->fetch_assoc();
        }

        if ($this->currentRow === null) {
            $this->lastSeenKeys = array();
        } else {
            foreach ($this->key as $index => $attribute) {
                $this->lastSeenKeys[$index] = $this->currentRow[$attribute];
            }
        }
    }

    private function alreadySeen()
    {
        foreach ($this->key as $index => $keyField) {
            if ($this->lastSeenKeys[$index] != $this->currentRow[$keyField]) {
                return false;
            }
        }

        return true;
    }

    public function first()
    {
        $this->rewind();

        return $this->valid() ? $this->current() : null;
    }

    public function fetchAll()
    {
        $nested = array_keys($this->nestedIterators);
        $retval = array();

        foreach ($this as $object) {
            foreach ($nested as $attributeName) {
                if (isset($object->$attributeName) && is_a($object->$attributeName, 'Iterator')) {
                    $object->$attributeName = $object->$attributeName->fetchAll();
                }
            }

            $retval[] = $object;
        }

        return $retval;
    }

    public function getNumRows()
    {
        return $this->resultSet !== null ? $this->resultSet->num_rows : null;
    }

    public function toArray()
    {
        return json_decode(json_encode($this->fetchAll()), false);
    }

    public function toJSON()
    {
        return json_encode($this->fetchAll());
    }
}
