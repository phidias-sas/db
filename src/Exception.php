<?php 
namespace Phidias\Db;

class Exception extends \Exception
{
    protected $data;

    public function __construct($data = null, $message = null, $code = 0, $previous = null)
    {
        $this->data = $data;

        parent::__construct($message, $code, $previous);
    }

    public function getData()
    {
        return $this->data;
    }
}
