<?php
/**
 * This file contains the definition for the Single class
 *
 */

namespace RyanWHowe\KeyValueStore\KeyValue;

/**
 * Class Single
 *
 * The single value class is the classic key/value store usage.  A key will have a
 * value set, which can be overwritten or deleted.
 *
 * @package RyanWHowe\KeyValueStore\KeyValue
 * @author  Ryan Howe <ryanwhowe@gmail.com>
 * @license MIT https://github.com/ryanwhowe/key-value-store/blob/master/LICENSE
 * @link    https://github.com/ryanwhowe/key-value-store/
 */
class Single extends \RyanWHowe\KeyValueStore\KeyValue
{

    /**
     * Set a new single grouping/key's value, if it is present, otherwise create a
     * new entry for it
     *
     * @param string $key   The key to set
     * @param string $value The value to set
     *
     * @return void
     * @throws \Doctrine\DBAL\DBALException
     */
    public function set($key, $value)
    {
        $tableId = $this->getId($key);
        if ($tableId) {
            $this->update($tableId, $value);
            return;
        }
        $this->insert($key, $value);
    }

    /**
     * Update a single grouping/key combination's value
     *
     * @param integer $tableId The id value for the table to use
     * @param string  $value   The value to set
     *
     * @return void
     */
    protected function update($tableId, $value)
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->update('ValueStore')
            ->set('value', ':value')
            ->where('id = :id')
            ->setParameter('value', $value, \PDO::PARAM_STR)
            ->setParameter('id', $tableId, \PDO::PARAM_INT);
        $queryBuilder->execute();
    }

    /**
     * Query the database for a single value from a grouping
     *
     * @param string $key The key to retrieve from the database
     *
     * @return bool|array
     */
    public function get($key)
    {
        $queryBuilder = $this->connection->createQueryBuilder();
        $queryBuilder->select('value')
            ->from('ValueStore')
            ->where('grouping = :grouping')
            ->andWhere('key = :key')
            ->setParameter(':grouping', $this->getGrouping(), \PDO::PARAM_STR)
            ->setParameter(':key', \strtolower($key), \PDO::PARAM_STR);
        $stmt = $queryBuilder->execute();
        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }
}