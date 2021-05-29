<?php

namespace Phidias\Db\JsonDb\Orm\Index;

class Entity extends \Phidias\Db\Orm\Entity
{
    protected static $schema = [
        "table" => "sophia_orm_table_indexes",
        "keys" => ["tableId", "recordId", "keyName", "keyValue"],

        "attributes" => [
            "tableId" => [
                "type" => "varchar",
                "length" => 32,
                "acceptNull" => false,
            ],

            "recordId" => [
                "entity" => "Phidias\\Db\\JsonDb\\Orm\\Record\\Entity",
                "onDelete" => "CASCADE",
                "onUpdate" => "CASCADE",
                "acceptNull" => false,
            ],

            "keyName" => [
                "type" => "varchar",
                "length" => 32,
                "acceptNull" => false,
            ],

            "keyValue" => [
                "type" => "varchar",
                "length" => 32,
                "acceptNull" => true,
            ]
        ],

        // "indexes" => [
        //     "tableId" => "tableId",
        //     "recordId" => "recordId",
        // ]
    ];
}
