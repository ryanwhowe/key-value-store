<?php
/**
 * This file contains the definition for the Series class
 *
 * PHP Version 5.3+
 *
 * @category File
 * @package  RyanWHowe\KeyValueStore
 * @author   Ryan W Howe <ryanwhowe@gmail.com>
 * @license  MIT https://github.com/ryanwhowe/key-value-store/blob/master/LICENSE
 * @link     https://github.com/ryanwhowe/key-value-store
 */

namespace RyanWHowe\KeyValueStore\KeyValue;

/**
 * Class Series is the instantiable representation of the Multi abstract class
 *
 * @category Class
 * @package  RyanWHowe\KeyValueStore
 * @author   Ryan W Howe <ryanwhowe@gmail.com>
 * @license  MIT https://github.com/ryanwhowe/key-value-store/blob/master/LICENSE
 * @link     https://github.com/ryanwhowe/key-value-store
 */
class Series extends \RyanWHowe\KeyValueStore\KeyValue
{
    /**
     * Insert a new value into a series set
     *
     * @param string $key   The key to set
     * @param string $value The value to set
     *
     * @return void
     */
    public function set($key, $value)
    {
        $this->insert($key, $value);
    }

    /**
     * Get the record associated with the specific key, value pair, the most recent
     * series value
     *
     * @param string $key The key to get the last set value for
     *
     * @return bool|array
     */
    public function get($key)
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->select(array('value', 'last_update'))
            ->from('ValueStore')
            ->where('grouping = :grouping')
            ->andWhere('key = :key')
            ->orderBy('id', 'DESC')
            ->setParameter(':grouping', $this->getGrouping(), \PDO::PARAM_STR)
            ->setParameter(':key', \strtolower($key), \PDO::PARAM_STR);
        $stmt = $queryBuilder->execute();
        $return = $stmt->fetch();
        if (is_array($return)) {
            $return['value_created'] = $this->getSeriesCreateDate($key);
        }
        return $return;
    }

    /**
     * Get the first value_created for a series collection
     *
     * @param string $key The key to get the series creation date for
     *
     * @return bool|string
     */
    protected function getSeriesCreateDate($key)
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->select('MIN(value_created)')
            ->from('ValueStore')
            ->where('grouping = :grouping')
            ->andWhere('key = :key')
            ->setParameter(':grouping', $this->getGrouping(), \PDO::PARAM_STR)
            ->setParameter(':key', \strtolower($key), \PDO::PARAM_STR);
        $stmt = $queryBuilder->execute();
        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }

    /**
     * Return the series set of values for the provided key
     *
     * @param string $key The key to get the series values for
     *
     * @return array
     */
    public function getSet($key)
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->select(array('value', 'last_update', 'value_created'))
            ->from('ValueStore')
            ->where('grouping = :grouping')
            ->andWhere('key = :key')
            ->orderBy('id', 'ASC')
            ->setParameter(':grouping', $this->getGrouping(), \PDO::PARAM_STR)
            ->setParameter(':key', \strtolower($key), \PDO::PARAM_STR);
        $stmt = $queryBuilder->execute();
        return $stmt->fetchAll();
    }
}