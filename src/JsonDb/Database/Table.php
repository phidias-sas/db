<?php

namespace Phidias\Db\JsonDb\Database;

use Phidias\Db\JsonDb\Orm\Controller as Orm;
use Phidias\Db\JsonDb\Orm\Record\Entity as Record;
use Phidias\Db\JsonDb\Orm\Index\Controller as Indexes;

class Table implements TableInterface
{
    private $tableName;
    private $attributes;
    private $collection;

    private $useAllAttributes;
    private $indexableProperties;

    public function __construct($tableName, $indexableProperties = [])
    {
        $this->tableName = $tableName;
        $this->indexableProperties = $indexableProperties;

        $this->attributes = [];
        $this->useAllAttributes = false;

        $this->collection = Record::collection()
            ->attribute("id")
            ->match("tableId", $this->tableName);
    }

    public function insert($data)
    {
        return Orm::postRecord($this->tableName, $data, $this->indexableProperties);
    }

    public function where($condition)
    {
    }

    public function attribute($attributeName)
    {
        $this->attributes[$attributeName] = $attributeName;

        switch ($attributeName) {
            case "id":
            case "authorId":
            case "dateCreated":
            case "dateModified":
                $this->collection->attribute($attributeName);
                break;

            case "*":
                $this->useAllAttributes = true;
                $this->collection->attribute("data");
                break;

            default:
                $this->collection->attribute("x.$attributeName", "JSON_EXTRACT(data, '$.$attributeName')");
                break;
        }

        return $this;
    }

    public function match($attributeName, $attributeValue)
    {
        switch ($attributeName) {
            case "id":
            case "authorId":
            case "dateCreated":
            case "dateModified":
                $this->collection->match($attributeName, $attributeValue);
                break;

            default:
                Indexes::filterCollection($this->collection, $this->tableName, $attributeName, $attributeValue);
                break;
        }

        return $this;
    }

    public function limit($limit)
    {
        $this->collection->limit($limit);
        return $this;
    }

    public function fetch()
    {
        $retval = [];

        foreach ($this->collection->find()->fetchAll() as $record) {
            if ($this->useAllAttributes) {
                $retvalItem = isset($record->data) && is_object($record->data) ? $record->data : new \stdClass;
                $retvalItem->id = $record->id;
            } else {
                $retvalItem = new \stdClass;
                $retvalItem->id = $record->id;

                foreach ($this->attributes as $attributeName) {
                    if (isset($record->$attributeName)) {
                        $retvalItem->$attributeName = $record->$attributeName;
                    } else if (isset($record->{"x." . $attributeName})) {
                        $retvalItem->$attributeName = json_decode($record->{"x." . $attributeName});
                        unset($record->{"x." . $attributeName});
                    }
                }
            }

            $retval[] = $retvalItem;
        }

        return $retval;
    }
}
