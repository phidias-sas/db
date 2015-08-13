<?php namespace Phidias\Db\Orm\Import;

class Target
{
    private $id;
    private $className;
    private $attributes;
    private $dependencies;
    private $keys;
    private $map;

    public function __construct($id, $className = null)
    {
        $this->id           = $id;
        $this->className    = $className;
        $this->attributes   = array();
        $this->dependencies = array();
        $this->keys         = array();
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

    public function map($map)
    {
        $this->map = $map;

        return $this;
    }

    public function attribute($attributeName, $attributeSource, $isKey = false)
    {
        if (is_object($attributeSource)) {

            if (!isset($attributeSource->type)) {
                return $this;
            }

            $isKey = (isset($attributeSource->isKey) && $attributeSource->isKey) || $isKey;

            if ($attributeSource->type == "column") {
                $attributeSource = "col:" . $attributeSource->value;
            } else {
                $attributeSource = $attributeSource->value;
            }
        }

        $this->attributes[$attributeName] = $attributeSource;

        if (substr($attributeSource, 0, 1) === "#") {
            $referencedTargetId = substr($attributeSource, 1, strpos($attributeSource, ".")-1);
            $this->dependencies[$referencedTargetId] = $referencedTargetId;
        }

        if ($isKey) {
            $this->keys[$attributeName] = $attributeName;
        }

        return $this;
    }

    public function addKey($attributeName)
    {
        if (isset($this->attributes[$attributeName])) {
            $this->keys[$attributeName] = $attributeName;
        }

        return $this;
    }

    public function attributes($attributes)
    {
        foreach ($attributes as $attributeName => $attributeValue) {
            if ($attributeValue === null) {
                continue;
            }
            $this->attribute($attributeName, $attributeValue);
        }

        return $this;
    }

    public function resolve($row, &$resolved)
    {
        if (isset($resolved[$this->id])) {

            if ($resolved[$this->id] === false) {
                throw new \Exception("circular target reference");
            }

            return;
        }

        $resolved[$this->id] = false;

        foreach ($this->dependencies as $referencedTargetId) {
            $this->map->getTarget($referencedTargetId)->resolve($row, $resolved);
        }

        $className = $this->className;

        if ($this->keys) {

            $entity = $className::single()->allAttributes();

            foreach ($this->keys as $keyAttributeName) {
                $keyValue = $this->resolveValue($this->attributes[$keyAttributeName], $row, $resolved);
                $entity->match($keyAttributeName, $keyValue);
            }

            $found  = $entity->find()->first();
            $entity = $found ? $found : new $className;

        } else {
            $entity = new $className;
        }

        foreach ($this->attributes as $attributeName => $attributeSource) {
            $entity->$attributeName = $this->resolveValue($attributeSource, $row, $resolved);
        }

        try {
            $entity->save();
        } catch (\Exception $e) {
            //zzzz
        }

        $resolved[$this->id] = $entity;
    }

    private function resolveValue($value, $row, $resolved)
    {
        //external reference
        if (substr($value, 0, 1) === "#") {

            list($referencedTargetId, $referencedTargetAttribute) = explode(".", substr($value, 1));

            if (!isset($resolved[$referencedTargetId]) || !$resolved[$referencedTargetId]) {
                return null;
            }

            return $resolved[$referencedTargetId]->$referencedTargetAttribute;
        }

        //column index
        if (strtolower(substr($value, 0, 4)) === "col:") {
            $columnIndex = substr($value, 4);
            return isset($row[$columnIndex]) ? $row[$columnIndex] : null;
        }

        //constant
        return $value;
    }

}