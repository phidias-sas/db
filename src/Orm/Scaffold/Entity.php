<?php
namespace Phidias\Db\Orm\Scaffold;

class Entity
{
    private $db;
    private $table;
    private $attributes;

    public function __construct()
    {
        $this->attributes   = array();
    }

    public function setDb($db)
    {
        $this->db = $db;
    }

    public function addAttribute($attributeData)
    {
        $this->attributes[$attributeData['Field']] = $attributeData;
    }

    public function fromTable($db, $table)
    {
        $this->table = $table;

        $result = $db->query("DESCRIBE $table");

        while ($row = $result->fetch_assoc()) {
            $this->addAttribute($row);
        }
    }

    public function save($namespace, $classname, $filename)
    {
        $keyAttributes = array();

        $output = "<?php\n\n";
        $output .= "namespace $namespace;\n\n";
        $output .= "class $classname extends \Phidias\Db\Orm\Entity \n";
        $output .= "{\n";

        foreach ($this->attributes as $attributeName => $attributeData) {
            $output .= "    var \${$attributeName};\n";

            if ($attributeData['Key'] == 'PRI') {
                $keyAttributes[] = $attributeName;
            }

        }

        $output .= "\n";
        $output .= "    protected static \$schema = [\n";
        $output .= "\n";

        if ($this->db) {
            $output .= "        'db' => '$this->db',\n";
        }

        $output .= "        'table' => '$this->table',\n";
        $output .= "        'keys' => ['".implode("', '", $keyAttributes)."'],\n";
        $output .= "\n";
        $output .= "        'attributes' => [\n\n";

        foreach ($this->attributes as $attributeName => $attributeData) {

            preg_match_all("/(.+)\((.+)\)/", $attributeData['Type'], $matches);

            if (!isset($matches[1][0])) {
                $type = $attributeData['Type'];
                $length = null;
            } else {
                $type   = $matches[1][0];
                $length = $matches[2][0];
            }

            $output .= "            '$attributeName' => [\n";
            $output .= "                'type' => '$type',\n";
            if ($length) {
                $output .= "                'length' => $length,\n";
            }

            if ($attributeData['Null'] == 'YES') {
                $output .= "                'acceptNull' => true,\n";
            }

            if ($attributeData['Default'] != null) {
                $output .= "                'default' => '{$attributeData['Default']}',\n";
            }

            $output .= "            ],\n\n";

        }
        $output .= "        ]\n";

        $output .= "    ];\n";

        $output .= "}";


        if (!is_dir(dirname($filename))) {
            mkdir(dirname($filename), 0777, true);
        }

        file_put_contents($filename, $output);
    }
}
