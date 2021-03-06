<?php
/**
 * This file contains the source code for the Store class
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
 * Class Store
 *
 * The Store class is access to a key value system stored in a database.  This class
 * will keep it's data separate from other stores through the use of the 'grouping'
 * term that is set at instantiation.
 *
 * @category Class
 * @package  RyanWHowe\KeyValueStore
 * @author   Ryan W Howe <ryanwhowe@gmail.com>
 * @license  MIT https://github.com/ryanwhowe/key-value-store/blob/master/LICENSE
 * @link     https://github.com/ryanwhowe/key-value-store
 */
abstract class KeyValue
{

    /**
     * The connection to the database
     *
     * @var \Doctrine\DBAL\Connection $connection
     */
    protected $connection;

    /**
     * The Grouping that the class will be associated with
     *
     * @var string $grouping
     */
    protected $grouping;

    /**
     * KeyValue constructor.  This is set to private to force instantiation through
     * the static create method
     *
     * @param string                    $grouping   The name of the grouping to
     *                                              instantiate the class
     * @param \Doctrine\DBAL\Connection $connection The passed connection
     */
    private function __construct($grouping, \Doctrine\DBAL\Connection $connection)
    {
        $this->grouping = self::formatGrouping($grouping);
        $this->connection = $connection;
    }

    /**
     * Static create method for single method chaining
     *
     * @param string                    $grouping   The name of the grouping to
     *                                              instantiate the class
     * @param \Doctrine\DBAL\Connection $connection The passed connection
     *
     * @return static
     * @throws \Exception on invalid connection
     */
    public static function create($grouping, \Doctrine\DBAL\Connection $connection)
    {
        return new static($grouping, $connection);
    }

    /**
     * Format the grouping name, eliminate spaces, replacing them with '_' and trim
     * the name for leading and trailing white space
     *
     * @param string $grouping The grouping string to format
     *
     * @return mixed
     */
    protected function formatGrouping($grouping)
    {
        return \str_replace(array(' '), '_', trim($grouping));
    }

    /**
     * Get the Grouping name for the class instance
     *
     * @return string
     */
    public function getGrouping()
    {
        return $this->grouping;
    }

    /**
     * Get all keys associated with the grouping that are stored in the database.
     * If no keys are present for the grouping then the method will return false;
     *
     * @return array|bool
     */
    public function getAllKeys()
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->select('DISTINCT key')
            ->from('ValueStore')
            ->where('`grouping` = :grouping')
            ->setParameter('grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt = $queryBuilder->execute();
        $result = $stmt->fetchAll(\PDO::FETCH_COLUMN);
        if (count($result) === 0) {
            $result = false;
        }
        return $result;
    }

    /**
     * Get the all values and keys associated with the grouping set.  For any Series
     * based values the single, most recent, last_updated value will be returned
     * with the key.
     *
     * @return array
     */
    public function getGroupingSet()
    {
        $result = array();
        $keys = $this->getAllKeys();
        foreach ($keys as $key) {
            $result[] = array_merge(
                array
                (
                    'key' => $key
                ),
                $this->get($key)
            );
        }

        return $result;
    }

    /**
     * Insert a new value in the database, either as a new series entry or a new
     * single value
     *
     * @param string $key   The grouping key to insert
     * @param string $value The value to insert
     *
     * @return void
     */
    protected function insert($key, $value)
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->insert('ValueStore')
            ->setValue('`grouping`', '?')
            ->setValue('`key`', '?')
            ->setValue('`value`', '?')
            ->setValue('last_update', 'strftime(\'%Y-%m-%d %H:%M:%f\',\'now\')')
            ->setValue('`value_created`', 'strftime(\'%Y-%m-%d %H:%M:%f\',\'now\')')
            ->setParameter(0, $this->getGrouping(), \PDO::PARAM_STR)
            ->setParameter(1, \strtolower($key), \PDO::PARAM_STR)
            ->setParameter(2, $value, \PDO::PARAM_STR);
        $queryBuilder->execute();
    }

    /**
     * Delete all rows in the database for a given key in a instantiated grouping
     *
     * @param string $key The grouping key to delete all references of
     *
     * @return void
     */
    public function delete($key)
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->delete('ValueStore')
            ->where('`grouping` = :grouping')
            ->where('`key` = :key')
            ->setParameter('grouping', $this->getGrouping(), \PDO::PARAM_STR)
            ->setParameter('key', \strtolower($key), \PDO::PARAM_STR);
        $queryBuilder->execute();
    }

    /**
     * Get the id value for a specific grouping, key combo, also as a check to see
     * if the data already exists in the table
     *
     * @param string      $key   The grouping key value to get
     * @param null|string $value The value to get try and get an id for
     *
     * @return bool|string
     */
    protected function getId($key, $value = null)
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->select('id')
            ->from('ValueStore')
            ->where('grouping = :grouping')
            ->andWhere('key = :key')
            ->setParameter('grouping', $this->getGrouping(), \PDO::PARAM_STR)
            ->setParameter('key', \strtolower($key), \PDO::PARAM_STR);
        if (null !== $value) {
            $queryBuilder->andWhere('value = :value');
            $queryBuilder->setParameter("value", $value, \PDO::PARAM_STR);
        }
        $stmt = $queryBuilder->execute();

        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }

    /**
     * Get the value associated with the passed Key reference
     *
     * @param string $key The key used to associate to the underlying value
     *
     * @return bool|array
     */
    public abstract function get($key);

    /**
     * Set the value for the passed key reference
     *
     * @param string $key   The key used to associate to the underlying value
     * @param string $value The associated value to set
     *
     * @return void
     */
    public abstract function set($key, $value);
}