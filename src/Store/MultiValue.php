<?php
/**
 * This file contains the definition for the MultiValue class
 *
 * @author Ryan Howe
 * @since  2018-10-12
 */

namespace ryanwhowe\KeyValueStore\Store;


abstract class MultiValue extends \ryanwhowe\KeyValueStore\KeyValue {


    /**
     * Query the database for the most recent series value from a grouping
     *
     * @param $key
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function getSeriesLastValue($key)
    {
        $sql = "
            SELECT
                `grouping`,
                `key`,
                `value`,
                `last_update`
            FROM 
                `ValueStore`
            WHERE
                `grouping` = :grouping AND 
                `key` = :key
            ORDER BY 
                value_created DESC 
        ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        $stmt->execute();
        $return = $stmt->fetch();
        $return['value_created'] = $this->getSeriesCreateDate($key);
        return $return;
    }

    /**
     * Get the first value_created for a series collection
     *
     * @param $key
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
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }

    /**
     * Get all the values associated with a series key for the grouping.
     *
     * @param $key
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function getSeriesSet($key)
    {
        $sql = "
            SELECT
                `grouping`,
                `key`,
                `value`,
                `last_update`,
                `value_created`
            FROM 
                `ValueStore`
            WHERE
                `grouping` = :grouping AND 
                `key` = :key
            ORDER BY 
                value_created DESC 
        ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetchAll();
    }
}