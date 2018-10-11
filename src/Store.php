<?php
/**
 * This file contains the source code for the Store class
 */

namespace ryanwhowe\KeyValueStore;

/**
 * Class Store
 *
 * The Store class is access to a key value system stored in a database.  This class will keep it's data
 * separate from other stores through the use of the 'grouping' term that is set at instantiation.
 *
 * @author  Ryan W Howe <ryanwhowe@gmail.com>
 * @package ryanwhowe\Store
 * @todo    convert the simple queries into query building
 */
class Store {
    /**
     * @var \Doctrine\DBAL\Connection
     */
    protected $connection;

    /**
     * @var string
     */
    protected $grouping;

    public function __construct($grouping, $connection)
    {
        $this->grouping = self::formatGrouping($grouping);
        $this->connection = $connection;
    }

    public static function formatGrouping($grouping)
    {
        return \str_replace(array(' '), '', $grouping);
    }

    public function setSingleValue($key, $value)
    {
        // if this exists already we want to update the record
        $id = $this->searchForKeyId($key);
        if ($id) {
            $sql = "
                UPDATE `ValueStore` SET `value` = :value 
                WHERE `grouping` = :grouping AND `key` = :key";
        } else {
            // else insert a new record
            $sql = "
                INSERT INTO `ValueStore` (`grouping`, `key`, `value`, `value_created`) VALUES
                    (:grouping, :key, :value, CURRENT_TIMESTAMP)";
        }
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(":grouping", $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(":key", $key, \PDO::PARAM_STR);
        $stmt->bindValue(":value", $value, \PDO::PARAM_STR);
        $stmt->execute();
    }

    /**
     * @param $key
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    private function searchForKeyId($key)
    {
        $sql = "SELECT id FROM `ValueStore` WHERE `grouping` = :grouping AND `key` = :key;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        $stmt->execute();
        $id = $stmt->fetch(\PDO::FETCH_COLUMN);
        return $id;
    }

    public function getGrouping()
    {
        return $this->grouping;
    }

    public function getSingleValue($key)
    {
        $sql = "SELECT `value` FROM `ValueStore` WHERE `grouping` = :grouping AND `key` = :key;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(":grouping", $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(":key", $key, \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }
}