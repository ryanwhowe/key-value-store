<?php
/**
 * This file contains the definition for the Series class
 *
 * @author Ryan Howe
 * @since  2018-10-11
 */

namespace RyanWHowe\KeyValueStore\KeyValue;


class Series extends Multi {
    /**
     * Insert a new value into a series set
     *
     * @param $key
     * @param $value
     * @throws \Doctrine\DBAL\DBALException
     */
    public function set($key, $value)
    {
        $this->insert($key, $value);
    }
}