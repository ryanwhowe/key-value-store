<?php
/**
 * This file contains the definition for the Manager class
 *
 * @author Ryan Howe
 * @since  2018-10-11
 */

namespace RyanWHowe\KeyValueStore;

/**
 * Class Manager
 *
 * @package RyanWHowe\KeyValueStore
 */
class Manager {

    /**
     * @var \Doctrine\DBAL\Connection
     */
    protected $connection;

    /**
     * Manager constructor.
     *
     * @param \Doctrine\DBAL\Connection $connection
     */
    public function __construct(\Doctrine\DBAL\Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * Static create method for single method chaining usage
     *
     * @param \Doctrine\DBAL\Connection $connection
     * @return self
     */
    public static function create(\Doctrine\DBAL\Connection $connection)
    {
        return new self($connection);
    }

    /**
     * Get all groupings that are stored in the database
     *
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

    /**
     * Create the production table
     * @throws \Doctrine\DBAL\DBALException
     */
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
        $this->connection->exec($sql);
    }

    /**
     * Drop the database table used for storing the data
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function dropTable()
    {
        $sql = "DROP TABLE `ValueStore`";
        $this->connection->exec($sql);
    }

}