<?php
/**
 * This file contains the definition for the Manager class
 *
 * @author Ryan Howe
 * @since  2018-10-11
 */

namespace ryanwhowe\KeyValueStore;

/**
 * Class Manager
 *
 * @package ryanwhowe\KeyValueStore
 */
class Manager {

    /**
     * @var \Doctrine\DBAL\Connection
     */
    protected $connection;

    public function __construct(\Doctrine\DBAL\Connection $connection)
    {
        $this->connection = $connection;
    }

    public static function create(\Doctrine\DBAL\Connection $connection)
    {
        return new self($connection);
    }

    /**
     * @param \Doctrine\DBAL\Connection $connection
     * @return array
     * @throws \Exception
     */
    public function getAllGroupings()
    {

        $sql = "SELECT DISTINCT `grouping` FROM `ValueStore`";
        $stmt = $this->connection->prepare($sql);
        $stmt->execute();
        return $stmt->fetchAll(\PDO::FETCH_COLUMN);
    }

    public function createTable()
    {
        $sql = "CREATE TABLE `ValueStore` (
              `id` INTEGER PRIMARY KEY AUTOINCREMENT ,
              `grouping` VARCHAR(100) DEFAULT NULL,
              `key` VARCHAR(100) DEFAULT NULL,
              `value` VARCHAR(300) DEFAULT NULL,
              `last_update` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              `value_created` DATETIME NOT NULL 
            );";
        $stmt = $this->connection->exec($sql);
    }

    public function dropTable()
    {
        $sql = "DROP TABLE `ValueStore`";
        $this->connection->exec($sql);
    }

}