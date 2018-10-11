<?php
/**
 * This file contains the definition for the SeriesValue class
 *
 * @author Ryan Howe
 * @since  2018-10-11
 */

namespace ryanwhowe\KeyValueStore\Store;


class SeriesValue extends \ryanwhowe\KeyValueStore\Store {
    /**
     * Insert a new value into a series set
     *
     * @param $key
     * @param $value
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     */
    public function set($key, $value)
    {
        $this->insert($key, $value);
        $result = $this->getSeriesLastValue($key);
        return $result;
    }

    public function get($key)
    {
        return $this->getSeriesLastValue($key);
    }

    public function getSet($key)
    {
        return $this->getSeriesSet($key);
    }

}