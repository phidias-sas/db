<?php
namespace Phidias\Db\Exception;
use Phidias\Db\Exception;

class DuplicateKey extends Exception
{
    public function __construct($data, $message)
    {
        $matches = array();
        preg_match_all("/Duplicate entry '(.+)' for key '(.+)'/", $message, $matches);

        $data = array(
            'key'   => isset($matches[2][0]) ? $matches[2][0] : null,
            'entry' => isset($matches[1][0]) ? $matches[1][0] : null
        );

        return parent::__construct($data, $message);
    }
}
