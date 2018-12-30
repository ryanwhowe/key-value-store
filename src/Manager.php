<?php
/**
 * This file contains the definition for the Manager class
 *
 * PHP Version 5.3+
 *
 * @category File
 * @package  RyanWHowe\KeyValueStore
 * @author   Ryan W Howe <ryanwhowe@gmail.com>
 * @license  MIT https://github.com/ryanwhowe/key-value-store/blob/master/LICENSE
 * @link     https://github.com/ryanwhowe/key-value-store
 */

namespace RyanWHowe\KeyValueStore;

/**
 * Class Manager
 *
 * @category Class
 * @package  RyanWHowe\KeyValueStore
 * @author   Ryan W Howe <ryanwhowe@gmail.com>
 * @license  MIT https://github.com/ryanwhowe/key-value-store/blob/master/LICENSE
 * @link     https://github.com/ryanwhowe/key-value-store
 */
class Manager
{

    /**
     * The connection for the manager's functions
     *
     * @var \Doctrine\DBAL\Connection
     */
    protected $connection;

    /**
     * Manager constructor.
     *
     * @param \Doctrine\DBAL\Connection $connection The injected connection
     */
    public function __construct(\Doctrine\DBAL\Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * Static create method for single method chaining usage
     *
     * @param \Doctrine\DBAL\Connection $connection The injected connection
     *
     * @return self
     */
    public static function create(\Doctrine\DBAL\Connection $connection)
    {
        return new self($connection);
    }

    /**
     * Get all groupings that are stored in the database
     *
     * @return array
     * @throws \Exception
     */
    public function getAllGroupings()
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->select('DISTINCT grouping')
            ->from('ValueStore');
        $stmt = $queryBuilder->execute();
        return $stmt->fetchAll(\PDO::FETCH_COLUMN);
    }

    /**
     * Create the production table
     *
     * @return void
     * @throws \Doctrine\DBAL\DBALException
     */
    public function createTable()
    {
        $sql = "CREATE TABLE `ValueStore` (
              `id` INTEGER PRIMARY KEY AUTOINCREMENT ,
              `grouping` VARCHAR(100) DEFAULT NULL,
              `key` VARCHAR(100) DEFAULT NULL,
              `value` VARCHAR(300) DEFAULT NULL,
              `last_update` TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              `value_created` TEXT NOT NULL 
            );";
        $this->connection->exec($sql);
        $sql = "
            CREATE INDEX IF NOT EXISTS `grkeix` ON `ValueStore` (`grouping`,`key`)";
        $this->connection->exec($sql);
        $sql = "
            CREATE INDEX IF NOT EXISTS `laupgrkeix` 
                ON `ValueStore` (`last_update`, `grouping`, `key`)";
        $this->connection->exec($sql);
    }

    /**
     * Drop the database table used for storing the data
     *
     * @return void
     * @throws \Doctrine\DBAL\DBALException
     */
    public function dropTable()
    {
        $sql = "DROP TABLE `ValueStore`";
        $this->connection->exec($sql);
    }

}