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

    private function __construct($grouping, $connection)
    {
        $this->grouping = self::formatGrouping($grouping);
        $this->connection = $connection;
    }

    /**
     * Static create method
     *
     * @param string $grouping
     * @param \Doctrine\DBAL\Connection $connection
     * @return self
     * @throws \Exception on invalid connection
     */
    public static function create($grouping, \Doctrine\DBAL\Connection $connection)
    {
        return new self($grouping, $connection);
    }

    public static function formatGrouping($grouping)
    {
        return \str_replace(array(' '), '', $grouping);
    }

    public function getGrouping()
    {
        return $this->grouping;
    }

    /**
     * Get all keys associated with the grouping that the class was instantiated as
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getAllKeys()
    {

        $sql = 'SELECT DISTINCT `key` FROM `ValueStore` WHERE `grouping` = :grouping;';
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }

    /**
     * Get the last updated values for all keys contained in a grouping set
     *
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getGroupingSet()
    {
        $sql = "
        SELECT 
            :grouping as `grouping`,
            keyset.`key`,
            (SELECT `value` FROM `ValueStore` WHERE grouping = :grouping and `key` = keyset.`key` ORDER BY `last_update` DESC LIMIT 1) as `value`,
            (SELECT `last_update` FROM `ValueStore` WHERE grouping = :grouping and `key` = keyset.`key` ORDER BY `last_update` DESC LIMIT 1) as `last_update` 
        FROM
            (SELECT DISTINCT `key` FROM `ValueStore` WHERE `grouping` = :grouping) as keyset
        ;
        ";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetchAll();
    }

    /**
     * Query the database for the most recent series value from a grouping
     *
     * @param $key
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function getSeriesLastValue($key)
    {
        $sql = "
            SELECT
                `grouping`,
                `key`,
                `value`,
                `last_update`
            FROM 
                `ValueStore`
            WHERE
                `grouping` = :grouping AND 
                `key` = :key
            ORDER BY 
                value_created DESC 
        ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        $stmt->execute();
        $return = $stmt->fetch();
        $return['value_created'] = $this->getSeriesCreateDate($key);
        return $return;
    }

    /**
     * Get all the values associated with a series key for the grouping.
     *
     * @param $key
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function getSeriesSet($key)
    {
        $sql = "
            SELECT
                `grouping`,
                `key`,
                `value`,
                `last_update`,
                `value_created`
            FROM 
                `ValueStore`
            WHERE
                `grouping` = :grouping AND 
                `key` = :key
            ORDER BY 
                value_created DESC 
        ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetchAll();
    }

    /**
     * Insert a new value in the database, either as a new series entry or a new single value
     *
     * @param $key
     * @param $value
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function insert($key, $value)
    {
        $sql = "INSERT INTO `ValueStore` (`grouping`, `key`, `value`, `value_created`) VALUES
                (:grouping, :key, :value, CURRENT_TIMESTAMP);";
        $stmt = $this->connection->prepare($sql);

        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        $stmt->bindValue(':value', $value, \PDO::PARAM_STR);
        $stmt->execute();
    }

    /**
     * Get the first value_created for a series collection
     *
     * @param $key
     * @return bool|string
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function getSeriesCreateDate($key)
    {
        $sql = "
            SELECT
                MIN(`value_created`)
            FROM 
                `ValueStore`
            WHERE
                `grouping` = :grouping AND 
                `key` = :key
        ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }

}