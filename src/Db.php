<?php
namespace Phidias\Db;

use Phidias\Utilities\Debugger as Debug;

/**
 * Wrapper for PHPs mysqli extension.
 */
class Db
{
    const KEYWORD_NULL    = "@@null@@";
    const KEYWORD_DEFAULT = "@@default@@";

    private static $credentials         = array();
    private static $identifierCallbacks = array();
    private static $instances           = array();

    private $mysqli;

    /**
     * Configure database access credentials
     *
     * Default connection settings:
     *
     * Db::configure([
     *     "host"     => $host,
     *     "username" => $username,
     *     "password" => $password,
     *     "database" => $database,
     *     "charset"  => $charset
     * ]);
     *
     * $db = Db::connect();
     *
     *
     * Connection identifier:
     *
     * Db::configure("test", [
     *     "host"     => $host,
     *     "username" => $username,
     *     "password" => $password,
     *     "database" => $database,
     *     "charset"  => $charset
     * ]);
     *
     * $test = Db::connect("test");
     *
     *
     * Identifier callback:
     *
     * Db::configure(function getIdentifierData($identifier) {
     *     echo "Looking for identifier $identifier";
     *     return [
     *         "host"     => ...
     *         "username" => ...
     *         "password" => ...
     *     ]
     * });
     *
     * $db = Db::connect();
     * > Looking for identifier null
     *
     * $db = Db::connect("me");
     * > Looking for identifier "me"
     *
     */
    public static function configure($identifier, $credentials = null)
    {
        if (is_array($identifier)) {
            self::$credentials[self::KEYWORD_DEFAULT] = self::sanitizeCredentialsArray($identifier);
            return;
        }

        if ($identifier === null) {
            $identifier = self::KEYWORD_DEFAULT;
        }

        if (is_array($credentials)) {
            self::$credentials[$identifier] = self::sanitizeCredentialsArray($credentials);
            return;
        }

        if (is_callable($identifier)) {
            self::$identifierCallbacks[] = $identifier;
        }
    }

    /**
     * Connect to the database defined by the given identifier
     *
     * Explicit connection settings:
     *
     * Db::connect(array(
     *     "host"     => $host,
     *     "username" => $username,
     *     "password" => $password,
     *     "database" => $database,
     *     "charset"  => $charset
     * ))
     *
     *
     * Connect to previously configured default settings (see Db::configure)
     * Db::connect()
     *
     * Connect to a previously configured database identifier (see Db::configure)
     * Db::connect("test")
     *
     * @param string $identifier String identifier set via setIdentifier, or found calling an identifier finder callback
     *
     * @return Db Db instance
     */
    public static function connect($identifier = null)
    {
        if (isset(self::$instances[$identifier])) {
            return self::$instances[$identifier];
        }

        $credentials = self::getCredentials($identifier);
        list($host, $username, $password, $database, $charset) = array_values($credentials);

        Debug::startBlock("connecting to Db: $username:*******@$host/$database", 'SQL');

        $mysqli = mysqli_init();
        if (!$mysqli) {
            die('mysqli_init failed');
        }

        if (!$mysqli->options(MYSQLI_OPT_LOCAL_INFILE, true)) {
            die('Setting MYSQLI_OPT_LOCAL_INFILE failed');
        }

        if (!$mysqli->real_connect($host, $username, $password, $database)) {
            throw new Exception\CannotConnect(null, mysqli_connect_error());
        }

        if ($charset !== null) {
            $mysqli->set_charset($charset);
        }
        Debug::endBlock();

        self::$instances[$identifier] = new Db($mysqli);

        return self::$instances[$identifier];
    }

    /**
     * Create the database (if not exists) defined for the given identifier
     *
     */
    public static function create($identifier = null, $collation = "utf8_general_ci")
    {
        $credentials = self::getCredentials($identifier);

        list($host, $username, $password, $database, $charset) = array_values($credentials);

        Debug::startBlock("attempting to create database: $username:*******@$host/$database", 'SQL');

        $mysqli = mysqli_init();
        if (!$mysqli) {
            die('mysqli_init failed');
        }

        if (!$mysqli->real_connect($host, $username, $password)) {
            throw new Exception\CannotConnect(null, mysqli_connect_error());
        }

        if ($charset !== null) {
            $mysqli->set_charset($charset);
        }

        $query = "CREATE DATABASE IF NOT EXISTS `$database` DEFAULT CHARACTER SET $charset COLLATE $collation";

        if (! $mysqli->query($query)) {
            throw self::obtainException($mysqli->errno, $mysqli->error, $query);
        }

        Debug::endBlock();
    }


    public static function getCredentials($identifier)
    {
        $identifierKey = $identifier === null ? self::KEYWORD_DEFAULT : $identifier;

        if (isset(self::$credentials[$identifierKey])) {
            return self::$credentials[$identifierKey];
        }

        $foundValidCredentials = false;

        foreach (self::$identifierCallbacks as $callback) {

            try {
                return self::sanitizeCredentialsArray(call_user_func($callback, $identifier));
            } catch (\Exception $e) {
                // continue
            }

        }

        throw new Exception($identifier, "no connection settings found");
    }


    private static function sanitizeCredentialsArray($array)
    {
        if (!is_array($array)) {
            throw new Exception($array, "invalid connection settings");
        }

        $retval = [
            "host"     => isset($array["host"])     ? $array["host"]     : null,
            "username" => isset($array["username"]) ? $array["username"] : null,
            "password" => isset($array["password"]) ? $array["password"] : null,
            "database" => isset($array["database"]) ? $array["database"] : null,
            "charset"  => isset($array["charset"])  ? $array["charset"]  : "utf8"
        ];

        if ($retval["host"] === null) {
            $retval["password"] = "******";
            throw new Exception($retval, "invalid connection settings");
        }

        return $retval;
    }



    public function __construct($mysqli)
    {
        $this->mysqli = $mysqli;
    }

    public function beginTransaction()
    {
        $this->mysqli->autocommit(false);
    }

    public function commit()
    {
        $this->mysqli->commit();
        $this->mysqli->autocommit(true);
    }

    public function rollback()
    {
        $this->mysqli->rollback();
        $this->mysqli->autocommit(true);
    }

    public function getNextInsertID($tableName)
    {
        $result = $this->mysqli->query("SHOW TABLE STATUS LIKE '$tableName'");
        $row    = $result->fetch_assoc();

        return isset($row['Auto_increment']) ? $row['Auto_increment'] : null;
    }

    public function getLastInsertID()
    {
        return $this->mysqli->insert_id;
    }

    public function affectedRows()
    {
        return $this->mysqli->affected_rows;
    }

    public function escapeString($string)
    {
        return $this->mysqli->real_escape_string($string);
    }

    /**
     * Run the given query
     *
     * Strings preceded with ":" are replaced with corresponging sanitized values given in the $parameters argument
     *
     * e.g.
     * query("INSERT INTO table VALUES (:firstName, :lastName)",  array(
     *  "firstName" => "D'angelo",
     *  "lastName" => "Piot'r"
     * ))
     *
     * will generate the query "INSERT INTO table VALUES ('D\'angelo', 'Piot\'r')";
     *
     */
    public function query($query, $parameters = null)
    {
        if (is_array($parameters)) {
            $query = $this->bindParameters($query, $parameters);
        }

        $queryLength = strlen($query);

        Debug::startBlock($queryLength > 10000 ? "[Query too long to debug ($queryLength characters)]" : $query, 'SQL');
        $result = $this->mysqli->query($query);
        Debug::endBlock();

        if ($result === false) {
            throw self::obtainException($this->mysqli->errno, $this->mysqli->error, $query);
        }

        return $result;
    }

    /**
     * Given a string with parameter names preceded with a colon E.G: "My name is :name"
     * and a hashed array of values E.G array('name' => 'Santiago')
     * replace the parameter name in the string with the sanitized value
     *
     * @param string $string          String to parametrize
     * @param Array  $parameterValues Array matching the parameter name in the string with the corresponding value
     *
     * @return string The string with sanitized parameters
     */
    public function bindParameters($string, $parameters)
    {
        $parameterNames     = array();
        $sanitizedValues    = array();

        foreach ($parameters as $key => $value) {
            $parameterNames[]   = ":$key";
            $sanitizedValues[]  = $this->sanitizeValue($value);
        }

        return str_replace($parameterNames, $sanitizedValues, $string);
    }

    /**
     * Sanitize the given value into a safe, insertable string
     *
     * e.g.
     * sanitizeValue("D'angelo");
     * returns 'D\'angelo'
     *
     */
    private function sanitizeValue($value)
    {
        if ($value === self::KEYWORD_NULL || is_null($value)) {

            return "NULL";

        } elseif ($value === self::KEYWORD_DEFAULT) {

            return "default";

        } elseif (is_int($value) || is_float($value)) {

            return $value;

        } elseif (is_string($value)) {

            if ($value[0] == '`' && $value[strlen($value) - 1] == '`') {
                return substr($value, 1, -1);
            }

            return "'".$this->escapeString($value)."'";

        } elseif (is_bool($value)) {

            return $value ? 1 : 0;

        } elseif (is_array($value)) {

            $sanitizedValues = array();
            foreach ($value as $subvalue) {
                $sanitizedValues[] = $this->sanitizeValue($subvalue);
            }

            return '('.implode(', ', $sanitizedValues).')';
        }

        return null;
    }

    /**
     * Sanitize the given string as a query property
     * i.e. enclose in backticks
     */
    private function sanitizeProperty($property)
    {
        if (is_array($property)) {
            $retval = array();
            foreach ($property as $propertyItem) {
                $retval[] = $this->sanitizeProperty($propertyItem);
            }

            return $retval;
        }

        return "`".trim($property, "` ")."`";
    }

    /**
     * Sanitize and insert into the specified table
     *
     * $db->insert('people', array('id', 'name'), array(
     *      array(1, 'Santiago'),
     *      array(2, 'Hugo'),
     *      .....
     * ));       //INSERT INTO people (`id`, `name`) VALUES (1, [Santiago]), (2, 'Hugo) ...
     */
    public function insert($tableName, array $columnNames, $records, $onDuplicate = false, $autoIncrementColumns = array())
    {
        $tableName            = $this->sanitizeProperty($tableName);
        $columnNames          = $this->sanitizeProperty($columnNames);
        $autoIncrementColumns = $this->sanitizeProperty($autoIncrementColumns);

        $sanitizedRecords = array();
        foreach ($records as $targetRecord) {

            $fullySanitized = true;
            foreach ($targetRecord as $key => $value) {

                $sanitizedValue = $this->sanitizeValue($value);

                if ($sanitizedValue === null) {
                    $fullySanitized = false;
                } else {
                    $targetRecord[$key] = $sanitizedValue;
                }
            }

            if ($fullySanitized) {
                $sanitizedRecords[] = '('.implode(', ', $targetRecord).')';
            }
        }

        if (!count($sanitizedRecords)) {
            throw new Exception\NothingToInsert('no records passed sanitation');
        }

        $query = $onDuplicate === "ignore" ? "INSERT IGNORE INTO $tableName" : "INSERT INTO $tableName";
        $query .= "\n (".implode(", ", $columnNames).") ";
        $query .= "\n VALUES \n";
        $query .= implode(', ', $sanitizedRecords);

        if ($onDuplicate === "update") {

            $updateFallbacks = array();

            foreach ($autoIncrementColumns as $columnName) {
                $updateFallbacks[] = "$columnName = LAST_INSERT_ID($columnName)";
            }

            foreach ($columnNames as $columnName) {
                if (in_array($columnName, $autoIncrementColumns)) {
                    continue;
                }

                $updateFallbacks[] = "$columnName = VALUES($columnName)";
            }

            if ($updateFallbacks) {
                $query .= "\n ON DUPLICATE KEY UPDATE \n";
                $query .= implode(', ', $updateFallbacks);
            }
        }

        $this->query($query);

        return $this->affectedRows();
    }

    public function insertUpdate($tableName, array $columnNames, $records, $autoIncrementColumns = array())
    {
        return $this->insert($tableName, $columnNames, $records, "update", $autoIncrementColumns);
    }

    public function insertIgnore($tableName, array $columnNames, $records)
    {
        return $this->insert($tableName, $columnNames, $records, "ignore");
    }

    /**
     * Update values in the given table
     *
     * $db->update("people", array("columnName" => "newValue"), "id = :id", array("id" => 7));
     * //UPDATE people SET `columnName` = 'newValue' WHERE `id` = 7
     *
     */
    public function update($tableName, array $values, $condition = null, $parameters = null)
    {
        if (stripos($tableName, "`") === false) {
            $tableName = $this->sanitizeProperty($tableName);
        }

        $valuesArray = array();

        foreach ($values as $columnName => $value) {

            $columnName     = $this->sanitizeProperty($columnName);
            $sanitizedValue = $this->sanitizeValue($value);

            if ($sanitizedValue !== null) {
                $valuesArray[] = "$columnName = $sanitizedValue";
            } else {
                trigger_error("could not sanitize value for column '$columnName'", E_USER_WARNING);
            }
        }

        if (!count($valuesArray)) {
            return 0;
        }

        $query = "UPDATE $tableName SET ".implode(', ', $valuesArray);

        if ($condition) {
            if (is_array($parameters)) {
                $condition = $this->bindParameters($condition, $parameters);
            }

            $query .= " WHERE $condition";
        }

        $this->query($query);

        return $this->affectedRows();
    }

    /**
     * Delete values from the given table
     *
     * $db->delete("people", "id = :id", array("id" => 7));
     * DELETE FROM `people` WHERE id = 7;
     *
     */
    public function delete($tableName, $condition = null, $parameters = null)
    {
        $tableName = $this->sanitizeProperty($tableName);

        $query = "DELETE FROM $tableName";

        if ($condition !== null) {
            if (is_array($parameters)) {
                $condition = $this->bindParameters($condition, $parameters);
            }
            $query .= " WHERE $condition";
        }

        $this->query($query);

        return $this->affectedRows();
    }

    /**
     * Return an exception corresponding to the given mariaDb error number an message
     *
     * http://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
     *
     */
    private static function obtainException($errno, $error, $query = null)
    {
        switch ($errno) {

            case 1048:
                $exception = new Exception\CannotBeNull($query, $error);
            break;

            case 1054:
                $exception = new Exception\UnknownColumn($query, $error);
            break;

            case 1062:
                $exception = new Exception\DuplicateKey($query, $error);
            break;

            case 1064:
                $exception = new Exception\ParseError($query, $error);
            break;

            case 1146:
                $exception = new Exception\UnknownTable($query, $error);
            break;

            case 1451:
                $exception = new Exception\ForeignKeyConstraint($query, $error);
            break;

            case 1452:
                $exception = new Exception\ReferenceNotFound($query, $error);
            break;

            default:
                $exception = new Exception($query, "Db error $errno: $error");
            break;

        }

        return $exception;
    }

}
