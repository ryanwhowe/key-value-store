<?php
/**
 * This file contains the source code for the Store class
 */

namespace RyanWHowe\KeyValueStore;

/**
 * Class Store
 *
 * The Store class is access to a key value system stored in a database.  This class will keep it's data
 * separate from other stores through the use of the 'grouping' term that is set at instantiation.
 *
 * @author  Ryan W Howe <ryanwhowe@gmail.com>
 * @package RyanWHowe\KeyValueStore
 */
abstract class KeyValue {

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
     * @return static
     * @throws \Exception on invalid connection
     */
    public static function create($grouping, \Doctrine\DBAL\Connection $connection)
    {
        return new static($grouping, $connection);
    }

    /**
     * Format the grouping name
     *
     * @param $grouping
     * @return mixed
     */
    protected function formatGrouping($grouping)
    {
        return \str_replace(array(' '), '_', trim($grouping));
    }

    /**
     * Get the Grouping name for the class instance
     * @return string
     */
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
        $qb = $this->connection->createQueryBuilder();
        $qb->select('DISTINCT key')
            ->from('ValueStore')
            ->where('`grouping` = :grouping')
            ->setParameter('grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt = $qb->execute();
        return $stmt->fetchAll(\PDO::FETCH_COLUMN);
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
            (SELECT `value` FROM `ValueStore` WHERE grouping = :grouping and `key` = keyset.`key` ORDER BY id DESC LIMIT 1) as `value`,
            (SELECT `last_update` FROM `ValueStore` WHERE grouping = :grouping and `key` = keyset.`key` ORDER BY id DESC LIMIT 1) as `last_update` 
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
     * Insert a new value in the database, either as a new series entry or a new single value
     *
     * @param $key
     * @param $value
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function insert($key, $value)
    {
        $qb = $this->connection->createQueryBuilder();
        $qb->insert('ValueStore')
            ->setValue('`grouping`', '?')
            ->setValue('`key`', '?')
            ->setValue('`value`', '?')
            ->setValue('`value_created`', 'CURRENT_TIMESTAMP')
            ->setParameter(0, $this->getGrouping(), \PDO::PARAM_STR)
            ->setParameter(1, $key, \PDO::PARAM_STR)
            ->setParameter(2, $value, \PDO::PARAM_STR);

        $qb->execute();
    }

    /**
     * Delete all data for a given key in a instantiated grouping
     *
     * @param $key
     */
    public function delete($key)
    {
        $qb = $this->connection->createQueryBuilder();
        $qb->delete('ValueStore')
            ->where('`grouping` = :grouping')
            ->where('`key` = :key')
            ->setParameter('grouping', $this->getGrouping(), \PDO::PARAM_STR)
            ->setParameter('key', $key, \PDO::PARAM_STR);
        $qb->execute();
    }
}