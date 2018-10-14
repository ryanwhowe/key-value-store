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

    /**
     * KeyValue constructor.  This is set to private to force instantiation through the static create method
     *
     * @param $grouping
     * @param $connection
     */
    private function __construct($grouping, $connection)
    {
        $this->grouping = self::formatGrouping($grouping);
        $this->connection = $connection;
    }

    /**
     * Static create method for single method chaining
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
     * Format the grouping name, eliminate spaces, replacing them with '_' and trim the name for leading and trailing
     * white space
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
     * Get all keys associated with the grouping that are stored in the database.
     *
     * @return array
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
     * Get the all values and keys associated with the grouping set.  For any Series based values the single, most
     * recent, last_updated value will be returned with the key.
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
            (SELECT `value` FROM `ValueStore` WHERE grouping = :grouping and `key` = keyset.`key` ORDER BY last_update DESC, id DESC LIMIT 1) as `value`,
            (SELECT `last_update` FROM `ValueStore` WHERE grouping = :grouping and `key` = keyset.`key` ORDER BY last_update DESC, id DESC LIMIT 1) as `last_update` 
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
     * Delete all rows in the database for a given key in a instantiated grouping
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

    /**
     * Get the id value for a specific grouping, key combo, also as a check to see if the data already
     * exists in the table
     *
     * @param  $key
     * @return bool|string
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function getId($key, $value = null)
    {
        $sql = "SELECT `id` FROM `ValueStore` WHERE `grouping` = :grouping AND `key` = :key ";
        if(null !== $value){
            $sql .= ' AND `value` = :value';
        }
        $stmt = $this->connection->prepare($sql);

        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        if(null !== $value){
            $stmt->bindValue(":value", $value, \PDO::PARAM_STR);
        }
        $stmt->execute();

        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }
}