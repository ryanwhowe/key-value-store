<?php
/**
 * This file contains the definition for the Multi class
 *
 */

namespace RyanWHowe\KeyValueStore\KeyValue;

/**
 * Class Multi
 *
 * This is the base abstract class that the multiple value classes extend from
 *
 * @package RyanWHowe\KeyValueStore\KeyValue
 * @author  Ryan Howe <ryanwhowe@gmail.com>
 * @license MIT https://github.com/ryanwhowe/key-value-store/blob/master/LICENSE
 * @link    https://github.com/ryanwhowe/key-value-store/
 */
abstract class Multi extends \RyanWHowe\KeyValueStore\KeyValue
{

    /**
     * Get the first value_created for a series collection
     *
     * @param string $key The key to get the series creation date for
     *
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
        $stmt->bindValue(':key', \strtolower($key), \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }

    /**
     * Get the record associated with the specific key, value pair, the most recent
     * series value
     *
     * @param string $key The key to get the last set value for
     *
     * @return bool|array
     * @throws \Doctrine\DBAL\DBALException
     */
    public function get($key)
    {
        $sql = "
        SELECT
            `value`,
            `last_update`
        FROM 
            `ValueStore`
        WHERE
            `grouping` = :grouping AND 
            `key` = :key
        ORDER BY 
            `id` DESC 
    ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', \strtolower($key), \PDO::PARAM_STR);
        $stmt->execute();
        $return = $stmt->fetch();
        if (is_array($return)) {
            $return['value_created'] = $this->getSeriesCreateDate($key);
        }
        return $return;
    }

    /**
     * Return the series set of values for the provided key
     *
     * @param string $key The key to get the series values for
     *
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getSet($key)
    {
        $sql = "
            SELECT
                `value`,
                `last_update`,
                `value_created`
            FROM 
                `ValueStore`
            WHERE
                `grouping` = :grouping AND 
                `key` = :key
            ORDER BY 
                `id` ASC
        ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', \strtolower($key), \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetchAll();
    }

}