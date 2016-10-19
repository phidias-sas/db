<?php namespace Phidias\Db\Orm\Import;

class Entity
{
    private $id;
    private $className;
    private $attributes;
    private $dependencies;
    private $keys;

    public function __construct($id, $className = null)
    {
        $this->id           = $id;
        $this->className    = $className;
        $this->attributes   = [];
        $this->dependencies = [];
        $this->keys         = [];
    }

    public function getId()
    {
        return $this->id;
    }

    public function className($className)
    {
        $this->className = $className;
        return $this;
    }


    public function attribute($name, $source, $value, $isKey = false)
    {
        $this->attributes[$name] = [
            "source" => $source, 
            "value"  => $value
        ];

        if ($source == "reference") {
            $referencedEntityId = substr($value, 0, strpos($value, "."));
            $this->dependencies[$referencedEntityId] = $referencedEntityId;
        }        

        if ($isKey) {
            $this->keys[$name] = $name;
        }

        return $this;
    }

    public function addKey($attributeName)
    {
        if (!isset($this->attributes[$attributeName])) {
            trigger_error("Cannot set key '$attributeName' because the attribute does not exist");
        }

        $this->keys[$attributeName] = $attributeName;
        return $this;
    }


    public function resolve($record, $map)
    {
        if (isset($map->resolved[$this->id])) {
            if ($map->resolved[$this->id] === false) {
                throw new \Exception("circular entity reference");
            }
            return;
        }

        $map->resolved[$this->id] = false;

        foreach ($this->dependencies as $referencedEntityId) {
            $map->getEntity($referencedEntityId)->resolve($record, $map);
        }

        $className = $this->className;

        if ($this->keys) {

            $entity = $className::single()->allAttributes();

            foreach ($this->keys as $keyAttributeName) {
                $keyValue = $this->resolveValue($this->attributes[$keyAttributeName], $record, $map->resolved);
                $entity->match($keyAttributeName, $keyValue);
            }

            $found  = $entity->find()->first();
            $entity = $found ? $found : new $className;
        } else {
            $entity = new $className;
        }

        foreach ($this->attributes as $attributeName => $attributeData) {
            $entity->$attributeName = $this->resolveValue($attributeData, $record, $map->resolved);
        }


        try {
            $entity->save();
            $map->resolved[$this->id] = $entity;
        } catch (\Exception $e) {
            $map->resolved[$this->id] = $e;
        }
    }

    private function resolveValue($attribute, $record, $resolved)
    {
        switch ($attribute["source"]) {
            case "reference":
                list($referencedEntityId, $referencedEntityAttribute) = explode(".", $attribute["value"]);
                if (!isset($resolved[$referencedEntityId]) || !$resolved[$referencedEntityId]) {
                    return null;
                }
                return $resolved[$referencedEntityId]->$referencedEntityAttribute;            

            case "column":
                $index = $attribute["value"];
                return isset($record[$index]) ? $record[$index] : null;

            default:
                return $attribute["value"];
        }
    }
}