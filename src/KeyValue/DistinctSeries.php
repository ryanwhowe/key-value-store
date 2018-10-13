<?php
/**
 * This file contains the definition for the DistinctSeries class
 *
 * @author Ryan Howe
 * @since  2018-10-11
 */

namespace RyanWHowe\KeyValueStore\KeyValue;

class DistinctSeries extends Multi {

    /**
     * Set a distinct series value, this will check to see if the key, value pair has already been submitted,
     * if not it will insert the value, if so it will issue an update which will update the last_updated timestamp
     *
     * @param $key
     * @param $value
     * @throws \Doctrine\DBAL\DBALException
     */
    public function set($key, $value)
    {
        $id = $this->getId($key, $value);
        if ($id) {
            $this->update($id);
        } else {
            $this->insert($key, $value);
        }
    }

    /**
     * Query the database to see if there is already a key, value pair that is the same as the distinct key,value passed
     * and if so, return the ID for that key, value pair
     *
     * @param $key
     * @param $value
     * @return bool|string
     * @throws \Doctrine\DBAL\DBALException
     */
    private function getId($key, $value)
    {

        $sql = "SELECT `id` FROM `ValueStore` WHERE `grouping` = :grouping AND `key` = :key AND `value` = :value ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':grouping', $this->getGrouping(), \PDO::PARAM_STR);
        $stmt->bindValue(':key', $key, \PDO::PARAM_STR);
        $stmt->bindValue(':value', $value, \PDO::PARAM_STR);
        $stmt->execute();
        return $stmt->fetch(\PDO::FETCH_COLUMN);
    }

    /**
     * Update the last_update for a unique value already in existence
     *
     * @param $id
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function update($id)
    {
        $sql = "UPDATE `ValueStore`
                SET 
                    `last_update` = CURRENT_TIMESTAMP
                WHERE
                    `id` = :id ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':id', $id, \PDO::PARAM_INT);
        $stmt->execute();
    }

    /**
     * Get the record associated with the specific key, value pair
     *
     * @param $key
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    public function get($key)
    {
        return $this->getSeriesLastValue($key);
    }

    /**
     * @param $key
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getSet($key)
    {
        return $this->getSeriesSet($key);
    }
}