<?php namespace Phidias\Db\Orm\Import;

/**
 * The import map is an utility to insert large sets of data entities
 * (e.g. craete entities from a .CSV file)
 *
 * Usage:
 *
 * source.csv:
 * id, firstName, father_id, father_firstName, father_lastName
 * 1,        foo,         2,              foo,             foo
 * 3,       shoo,         4,             shoo,            shoo
 * .... large number of rows
 *
 *
 * $map = new Map;
 *
 *
 * $map->target("father")
 *         ->className("Phidias\Core\Person\Entity")
 *         ->attribute("gender",     1)
 *         ->attribute("id",        "col:2", true)
 *         ->attribute("firstName", "col:3")
 *         ->attribute("lastName",  "col:4");
 *
 * $map->target("student")
 *         ->className("Phidias\Core\Person\Entity")
 *         ->attribute("id",        "col:0", true)
 *         ->attribute("firstName", "col:1")
 *         ->attribute("lastName",  "#father.lastName");
 *
 * $map->target("fatherRelation")
 *         ->className("Phidias\Core\Person\Relation\Entity")
 *         ->attribute("person",   "#student.id")
 *         ->attribute("relative", "#father.id")
 *         ->attribute("role",     1);
 *
 *
 *
 * foreach ($row in $importedCsvOrWhatever) {
 *     $createdEntities = $map->import($row);
 *
 *     echo "Father set for student ".$createdEntities["student"]->id;
 * }
 *
 */

class Map
{
    private $targets;

    public function __construct()
    {
        $this->targets = array();
    }

    public function addTarget(Target $target)
    {
        $target->map($this);
        $this->targets[$target->getId()] = $target;

        return $this;
    }

    public function target($targetId)
    {
        $target = new Target($targetId);
        $this->addTarget($target);

        return $target;
    }

    public function getTarget($targetId)
    {
        if (!isset($this->targets[$targetId])) {
            throw new \Exception("import target '$targetId' not found");
        }

        return $this->targets[$targetId];
    }

    public function import($row)
    {
        $resolved = array();

        foreach ($this->targets as $target) {
            $target->resolve($row, $resolved);
        }

        return $resolved;
    }

}

