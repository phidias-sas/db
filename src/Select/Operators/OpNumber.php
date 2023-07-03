<?php

namespace Phidias\Db\Select\Operators;

use Phidias\Db\Select\Utils;

class OpNumber
{
    private static function sanitizeField($fieldName)
    {
        return "CAST($fieldName as signed)";
    }

    public static function eq($fieldName, $args)
    {
        $fieldName = self::sanitizeField($fieldName);
        $args = Utils::escape($args);

        return "$fieldName = $args";
    }

    public static function gt($fieldName, $args)
    {
        $fieldName = self::sanitizeField($fieldName);
        $args = Utils::escape($args);

        return "$fieldName > $args";
    }

    public static function gte($fieldName, $args)
    {
        $fieldName = self::sanitizeField($fieldName);
        $args = Utils::escape($args);

        return "$fieldName >= $args";
    }

    public static function lt($fieldName, $args)
    {
        $fieldName = self::sanitizeField($fieldName);
        $args = Utils::escape($args);

        return "$fieldName < $args";
    }

    public static function lte($fieldName, $args)
    {
        $fieldName = self::sanitizeField($fieldName);
        $args = Utils::escape($args);

        return "$fieldName <= $args";
    }

    public static function between($fieldName, $args)
    {
        if (!is_array($args) || count($args) != 2) {
            return "0";
        }
        $fieldName = self::sanitizeField($fieldName);
        return $fieldName . " BETWEEN " . Utils::escape($args[0])  . " AND " . Utils::escape($args[1]);
    }
}
