<?php

namespace Phidias\Db\Select\Operators;

use Phidias\Db\Select\Utils;

class OpString
{
    public static function same($fieldName, $args)
    {
        $args = Utils::escape($args);
        return "$fieldName = $args";
    }

    public static function like($fieldName, $args)
    {
        $args = Utils::escape($args);
        return "$fieldName LIKE $args";
    }

    public static function eq($fieldName, $args)
    {
        $args = Utils::escape($args);
        return "$fieldName = $args";
    }

    public static function neq($fieldName, $args)
    {
        $args = Utils::escape($args);
        return "$fieldName != $args";
    }

    public static function includes($fieldName, $args)
    {
        $args = Utils::escape_string($args);
        return "CAST($fieldName as CHAR) LIKE '%$args%'";
    }

    public static function startsWith($fieldName, $args)
    {
        $args = Utils::escape_string($args);
        return "$fieldName LIKE '$args%'";
    }

    public static function endsWith($fieldName, $args)
    {
        $args = Utils::escape_string($args);
        return "$fieldName LIKE '%$args'";
    }

    public static function isEmpty($fieldName, $args)
    {
        return "($fieldName = '' OR $fieldName IS NULL)";
    }

    public static function nempty($fieldName, $args)
    {
        return "($fieldName != '' AND $fieldName IS NOT NULL)";
    }
}
