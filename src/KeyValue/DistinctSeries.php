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
}