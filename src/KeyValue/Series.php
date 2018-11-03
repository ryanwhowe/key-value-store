<?php
/**
 * This file contains the definition for the Series class
 *
 */

namespace RyanWHowe\KeyValueStore\KeyValue;

/**
 * Class Series
 *
 * @package RyanWHowe\KeyValueStore\KeyValue
 * @author  Ryan Howe <ryanwhowe@gmail.com>
 * @license MIT https://github.com/ryanwhowe/key-value-store/blob/master/LICENSE
 * @link    https://github.com/ryanwhowe/key-value-store/
 */
class Series extends Multi
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
}