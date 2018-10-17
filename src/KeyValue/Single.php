<?php
/**
 * This file contains the definition for the Single class
 *
 * @author Ryan Howe
 * @since  2018-10-11
 */

namespace RyanWHowe\KeyValueStore\KeyValue;

/**
 * Class Single
 *
 * The single value class is the classic key/value store usage.  A key will have a value set, which can be overwritten
 * or deleted.
 *
 * @package RyanWHowe\KeyValueStore\KeyValue
 */
class Single extends \RyanWHowe\KeyValueStore\KeyValue {

    /**
     * Set a new single grouping/key's value, if it is present, otherwise create a new entry for it
     *
     * @param $key
     * @param $value
     * @throws \Doctrine\DBAL\DBALException
     */
    public function set($key, $value)
    {
        $id = $this->getId($key);
        if ($id) {
            $this->update($id, $value);
        } else {
            $this->insert($key, $value);
        }
    }

    /**
     * Update a single grouping/key combination's value
     *
     * @param $id
     * @param $value
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function update($id, $value)
    {
        $sql = "UPDATE `ValueStore` 
                SET 
                    `value` = :value
                WHERE
                    `id` = :id ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':id', $id, \PDO::PARAM_INT);
        $stmt->bindValue(':value', $value, \PDO::PARAM_STR);
        $stmt->execute();
    }

    /**
     * Query the database for a single value from a grouping
     *
     * @param $key
     * @return bool|array
     * @throws \Doctrine\DBAL\DBALException
     */
    public function get($key)
    {
        $sql = "
            SELECT
                `value`
            FROM 
                `ValueStore`
            WHERE
                `grouping` = :grouping AND 
                `key` = :key
        ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', \strtolower($key), \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }
}