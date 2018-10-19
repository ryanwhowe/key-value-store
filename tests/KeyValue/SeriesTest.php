<?php
/**
 * This file contains
 *
 * @author Ryan Howe
 * @since  2018-10-12
 */

namespace Test\KeyValue;

use RyanWHowe\KeyValueStore\KeyValue\Series;

class SeriesTest extends DataTransaction {

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Multi::get
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Multi::getSeriesCreateDate
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Series::set
     * @dataProvider setGetDataProvider
     * @param string $key
     * @param array $values
     * @throws \Exception
     */
    public function set($key, $values)
    {
        $testGrouping = 'SeriesValueGet';
        $value = '';
        $seriesValue = Series::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $seriesValue->set($key, $item);
            $value = $item; //the expected output is the last value that was set in the series
        }
        $result = $seriesValue->get($key);
        unset($result['last_update']);
        unset($result['value_created']);
        $this->assertEquals(array('value' => $value), $result);
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Multi::getSet
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Series::set
     * @dataProvider multiKeyDataProvider
     * @param array $testSet
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function getSet($testSet)
    {
        $testGrouping = 'SeriesValueGetSet';
        $seriesValue = Series::create($testGrouping, self::$connection);

        foreach ($testSet as $item) {
            $expected = array();
            $key = $item['key'];
            foreach ($item['values'] as $value) {
                $seriesValue->set($key, $value);
                $expected[] = array('value' => $value);
            }

            $result = $seriesValue->getSet($key);
            foreach ($result as &$item) {
                unset($item['last_update']);
                unset($item['value_created']);
            }
            $this->assertEquals($expected, $result);
        }
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getAllKeys
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Series::set
     * @dataProvider multiKeyDataProvider
     * @param array $testSet
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function GetAllKeys($testSet)
    {
        $seriesValue = Series::create('SeriesValueGetAllKeys', self::$connection);
        $expected = array();
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $seriesValue->set($key, $value);
            }
            $expected[] = \strtolower($key);
            $result = $seriesValue->getAllKeys();
            $this->assertEquals($expected, $result);
        }
        $result = $seriesValue->getAllKeys();
        $this->assertEquals($expected, $result);
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @throws \Exception
     */
    public function create()
    {
        $testGroupName = 'SeriesValueCreate';
        $seriesValue = Series::create($testGroupName, self::$connection);
        $resultGroupName = $seriesValue->getGrouping();
        $this->assertEquals($testGroupName, $resultGroupName);
        $this->assertInstanceOf('RyanWHowe\KeyValueStore\KeyValue\Series', $seriesValue);
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGroupingSet
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Series::set
     * @dataProvider multiKeyDataProvider
     * @param array $testSet
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function getGroupingSet($testSet)
    {
        $testGroup = 'SeriesValueGetGroupingSet';
        $singleValue = Series::create($testGroup, self::$connection);
        $expected = array();
        $expected_value = '';
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $singleValue->set($key, $value);
                $expected_value = $value; // we expect the last value set
            }
            $expected[] = array('key' => \strtolower($key), 'value' => $expected_value);
        }

        $result = $singleValue->getGroupingSet();

        foreach ($result as &$item) {
            // We are removing the last_update, this is a timestamp and is not testable
            unset($item['last_update']);
        }

        $this->assertEquals($expected, $result);
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Multi::get
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Multi::getSeriesCreateDate
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Series::set
     * @dataProvider setGetDataProvider
     * @param string $key
     * @param array $values
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function get($key, $values)
    {
        $testGrouping = 'SeriesValueGet';
        $value = '';
        $seriesValue = Series::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $seriesValue->set($key, $item);
            $value = $item; //the expected output is the last value that was set in the series
        }
        $result = $seriesValue->get($key);
        unset($result['last_update']);
        unset($result['value_created']);
        $this->assertEquals(array('value' => $value), $result);
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @dataProvider groupingTestProvider
     * @throws \Exception
     */
    public function GetGrouping($testGroup, $expectedGroup, $expectedResult)
    {
        $singleValue = Series::create($testGroup, self::$connection);
        $resultGroup = $singleValue->getGrouping();
        if ($expectedResult) {
            $this->assertEquals($expectedGroup, $resultGroup);
        } else {
            $this->assertNotEquals($expectedGroup, $resultGroup);
        }
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::delete
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Multi::get
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Series::set
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function delete()
    {
        $testGroup = 'SeriesValueDelete';
        $key = 'KeyValue';
        $values = array('value1', 'value2', 'value3', 'value3', 'value2', 'value2', 'value1');
        $seriesValue = Series::create($testGroup, self::$connection);
        foreach ($values as $value) {
            $seriesValue->set($key, $value);
        }
        $seriesValue->delete($key);
        $result = $seriesValue->get($key);
        $this->assertFalse($result);
    }

    /**
     * @test
     * @covers       \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers       \RyanWHowe\KeyValueStore\Manager::create
     * @covers       \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers       \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::getAllKeys
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\Series::set
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\Series::update
     * @dataProvider nonUniqueKeyDataProvider
     * @throws \Exception
     */
    public function uniqueKeysCheck($testSet)
    {
        $testGroup = 'SingleUniqueKeys';
        $series = Series::create($testGroup, self::$connection);
        $expected = array();
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $series->set($key, $value);
            }
            $expected[\strtolower($key)] = true;
            $result = $series->getAllKeys();
            $this->assertEquals(array_keys($expected), $result);
        }

    }
}
