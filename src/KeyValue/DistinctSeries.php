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
     * @param string $key
     * @param string $value
     * @throws \Doctrine\DBAL\DBALException
     */
    public function set($key, $value)
    {
        $tableId = $this->getId($key, $value);
        if ($tableId) {
            $this->update($tableId);
        } else {
            $this->insert($key, $value);
        }
    }

    /**
     * Update the last_update for a unique value already in existence
     *
     * @param $tableId
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function update($tableId)
    {
        $sql = "UPDATE `ValueStore`
                SET 
                    `last_update` = CURRENT_TIMESTAMP
                WHERE
                    `id` = :id ;";
        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(':id', $tableId, \PDO::PARAM_INT);
        $stmt->execute();
    }

    /**
     * Get the record associated with the specific key, value pair, the last set value for the distinct series
     *
     * @param $key
     * @return bool|array
     * @throws \Doctrine\DBAL\DBALException
     */
    public function get($key)
    {
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
                `last_update` DESC
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
    }

    /**
     * Get the last unique value added to the distinct series set.  This differs from the get() in that it returns the
     * last by id, instead of last by last_update
     *
     * @param $key
     * @return array|bool
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getLastUnique($key)
    {
        return parent::get($key);
    }

}