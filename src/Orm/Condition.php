<?php

namespace Phidias\Db\Orm;

class Condition
{
    public static function applyPrimitive($collection, $condition)
    {
        $field = $condition->field;
        $args = $condition->args;

        /* Legacy conditions */
        switch ($condition->op) {
            case 'gt':
                return $collection->where("{$field} > :args", ["args" => $args]);

            case 'gte':
                return $collection->where("{$field} >= :args", ["args" => $args]);

            case 'lt':
                return $collection->where("{$field} < :args", ["args" => $args]);

            case 'lte':
                return $collection->where("{$field} <= :args", ["args" => $args]);

            case 'in':
                return $collection->where("{$field} IN :args", ["args" => $args]);

            case 'nin':
                return $collection->where("{$field} NOT IN :args", ["args" => $args]);

            case 'ne':
                return $collection->where("{$field} != :args", ["args" => $args]);

            case 'eq':
                return $collection->where("{$field} = :args", ["args" => $args]);

            case 'like':
                return $collection->where("{$field} LIKE :args", ["args" => $args]);
        }

        switch ($condition->op) {
            case 'string.eq':
            case 'number.eq':
            case 'date.eq':
                $collection->where("{$field} = :args", ["args" => $args]);
                break;

            case 'string.gt':
            case 'number.gt':
            case 'date.gt':
                $collection->where("{$field} > :args", ["args" => $args]);
                break;

            case 'string.gte':
            case 'number.gte':
            case 'date.gte':
                $collection->where("{$field} >= :args", ["args" => $args]);
                break;

            case 'string.lt':
            case 'number.lt':
            case 'date.lt':
                $collection->where("{$field} < :args", ["args" => $args]);
                break;

            case 'string.lte':
            case 'number.lte':
            case 'date.lte':
                $collection->where("{$field} <= :args", ["args" => $args]);
                break;

            case 'string.like':
                $collection->where("{$field} LIKE :args", ["args" => $args]);
                break;

            case 'string.same':
                $collection->where("{$field} LIKE :args", ["args" => $args . "%"]);
                break;

            case 'boolean.true':
                $collection->where("{$field} = 1");
                break;

            case 'boolean.false':
                $collection->where("{$field} = 0");
                break;
        }

        return $collection;
    }
}
