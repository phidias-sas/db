<?php namespace Phidias\Db\Orm\Import;

/**
The import map is an utility to import large sets of data into entities
(e.g. create or update entities from a .CSV file)

Usage:

source.csv:

id, firstName, father_id, father_firstName, father_lastName
1,        foo,         2,              foo,             foo
3,       shoo,         4,             shoo,            shoo
.... large number of rows


$map = new Map;

$map->entity("person")
    ->className("Phidias\Core\Person\Entity")
    ->attribute("document",    "column", 0, true)
    ->attribute("gender",      "value",  "male")
    ->attribute("firstName",   "column", 1)
    ->attribute("lastName",    "column", 2)
    ->attribute("description", "value",  "bla bla bla")

    ->attribute(<name of the entity's attribute>, <source: value, column, or reference>,  <value (as interpreted according to the source)>,  <is key>)

$map->entity("son")
    ->className("Phidias\Core\Person\Entity")
    ->attribute("document",  "column",    3, true)
    ->attribute("firstName", "column",    4)
    ->attribute("lastName",  "reference", "person.lastName");

$map->entity("fatherRelation")
    ->className("Phidias\Core\Person\Relation\Entity")
    ->attribute("person",   "reference", "person.id")
    ->attribute("relative", "reference", "son.id")
    ->attribute("role",     "value",     "child");


$record = [111, "Alfredo", "Cortes", 222, "Santiago"];

$map->import($record);



foreach ($record in $importedCsvOrWhatever) {
  $createdEntities = $map->import($record);

  echo "Father set for student ".$createdEntities["student"]->id;
}
 *
 */

class Map
{
    private $entities;
    public $resolved;

    public function __construct()
    {
        $this->entities = [];
    }

    public function addEntity(Entity $entity)
    {
        $this->entities[$entity->getId()] = $entity;
        return $this;
    }

    public function entity($entityId)
    {
        $entity = new Entity($entityId);
        $this->addEntity($entity);

        return $entity;
    }

    public function getEntity($entityId)
    {
        if (!isset($this->entities[$entityId])) {
            throw new \Exception("entity '$entityId' not found in import map");
        }

        return $this->entities[$entityId];
    }

    public function import($record)
    {
        $this->resolved = [];
        foreach ($this->entities as $entity) {
            $entity->resolve($record, $this);
        }
        return $this->resolved;
    }
}