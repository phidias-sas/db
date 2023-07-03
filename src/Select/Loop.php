<?php

namespace Phidias\Db\Select;

class Loop
{
    public $key;
    public $output;

    public static function each($key, $output)
    {
        return new self($key, $output);
    }

    public function __construct($key, $output)
    {
        $this->key = $key;
        $this->output = $output;
    }

    public function fetch_all($mysqli_result)
    {
        $retval = [];
        while ($row = $mysqli_result->fetch_assoc()) {
            $this->processRow($row, $retval);
        }
        return $this->unhash($retval);
    }

    private function processRow($row, &$retval)
    {
        if ($this->key) {
            if (!isset($row[$this->key])) {
                // warning: Row does not contain key '$this->key'
                return;
            }
            $keyValue = $row[$this->key];
            if (!isset($retval[$keyValue])) {
                $retval[$keyValue] = (object)[];
            }
            $curItem = $retval[$keyValue];
        } else {
            $curItem = (object)[];
            $retval[] = $curItem;
        }

        $this->hydrateItem($row, $this->output, $curItem);
    }

    private function hydrateItem($row, $output, &$item)
    {
        foreach ($output as $propName => $propSource) {
            if ($propSource instanceof self) {
                $item->$propName = $item->$propName ?? [];
                $propSource->processRow($row, $item->$propName);
            } elseif ($propSource instanceof Transform) {
                $item->$propName = $propSource->execute($row);
            } elseif (is_string($propSource)) {
                $item->$propName = $row[$propSource] ?? null;
            } elseif (is_object($propSource) || is_array($propSource)) {
                $item->$propName = $item->$propName ?? (object)[];
                $this->hydrateItem($row, $propSource, $item->$propName);
            }
        }
    }

    private function unhash($thing)
    {
        if (is_array($thing)) {
            return array_map(function ($value) {
                return $this->unhash($value);
            }, array_values($thing));
        }

        if (is_object($thing)) {
            $retval = (object)[];
            foreach ($thing as $propName => $propValue) {
                $retval->$propName = $this->unhash($propValue);
            }
            return $retval;
        }

        return $thing;
    }
}
