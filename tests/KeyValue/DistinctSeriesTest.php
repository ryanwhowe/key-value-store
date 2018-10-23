<?php
/**
 * This file contains
 *
 * @author Ryan Howe
 * @since  2018-10-13
 */

namespace Test\KeyValue;

use RyanWHowe\KeyValueStore\KeyValue\DistinctSeries;

class DistinctSeriesTest extends DataTransaction {

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
        $singleValue = DistinctSeries::create($testGroup, self::$connection);
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
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::update
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::get
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function delete()
    {
        $testGroup = 'SeriesValueDelete';
        $key = 'KeyValue';
        $values = array('value1', 'value2', 'value3', 'value3', 'value2', 'value2', 'value1');
        $distinctSeries = DistinctSeries::create($testGroup, self::$connection);
        foreach ($values as $value) {
            $distinctSeries->set($key, $value);
        }
        $distinctSeries->delete($key);
        $result = $distinctSeries->get($key);
        $this->assertFalse($result);
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
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::update
     * @dataProvider multiKeyDataProvider
     * @param array $testSet
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function getGroupingSet($testSet)
    {
        $testGroup = 'DistinctSeriesValueGetGroupingSet';

        $seriesValue = DistinctSeries::create($testGroup, self::$connection);
        $expected = array();
        $expected_values = array();
        foreach ($testSet as $item) {
            $key = $item['key'];
            foreach ($item['values'] as $value) {
                $seriesValue->set($key, $value);
                if ( ! array_key_exists($value, $expected_values)) {
                    $expected_values[$value] = true;
                }
            }
            $expected_values = \array_keys($expected_values);
            $expected_value = end($expected_values);
            $expected[] = array('key' => \strtolower($key), 'value' => $expected_value);
        }

        $result = $seriesValue->getGroupingSet();

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
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::update
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::get
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Multi::getSeriesCreateDate
     * @dataProvider setGetDataProvider
     * @param string $key
     * @param array $values
     * @throws \Exception
     * @throws \Doctrine\DBAL\DBALException
     */
    public function get($key, $values)
    {
        $testGrouping = 'DistinctSeriesValueGet';
        $expected_value = '';

        $distinctSeriesValue = DistinctSeries::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $distinctSeriesValue->set($key, $item);
            /* The sleep is needed to have the sqlite database see a difference in timestamp values*/
            \sleep(1);
            $expected_value = $item;
        }
        $result = $distinctSeriesValue->get($key);
        unset($result['last_update']);
        unset($result['value_created']);

        $this->assertEquals(array('value' => $expected_value), $result);
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
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::update
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Multi::getSet
     * @dataProvider multiKeyDataProvider
     * @param array $testSet
     * @throws \Exception
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getSet($testSet)
    {
        $testGrouping = 'DistinctSeriesValueGetSet';

        $distinctSeries = DistinctSeries::create($testGrouping, self::$connection);

        foreach ($testSet as $item) {
            $expected = array();
            $expected_values = array();
            $key = $item['key'];
            foreach ($item['values'] as $value) {
                $distinctSeries->set($key, $value);
                if ( ! array_key_exists($value, $expected_values)) {
                    $expected_values[$value] = true;
                }
            }
            foreach ($expected_values as $values => $item) {
                $expected[] = array('value' => $values);
            }
            $result = $distinctSeries->getSet($key);
            // unset the timestamps, which will vary with time
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
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::update
     * @dataProvider multiKeyDataProvider
     * @param array $testSet
     * @throws \Exception
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getAllKeys($testSet)
    {
        $distinctSeries = DistinctSeries::create('DistinctSeriesValueGetAllKeys', self::$connection);

        $expected = array();
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $distinctSeries->set($key, $value);
            }
            $expected[] = \strtolower($key);
            $result = $distinctSeries->getAllKeys();
            $this->assertEquals($expected, $result);
        }
        $result = $distinctSeries->getAllKeys();
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
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::update
     * @covers \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::get
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Multi::getSeriesCreateDate
     * @dataProvider setGetDataProvider
     * @param string $key
     * @param array $values
     * @throws \Exception
     * @throws \Doctrine\DBAL\DBALException
     */
    public function set($key, $values)
    {
        $testGrouping = 'DistinctSeriesValueSet';
        $expected_value = '';
        $distinctSeriesValue = DistinctSeries::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $distinctSeriesValue->set($key, $item);
            /* The sleep is needed to have the sqlite database see a difference in timestamp values*/
            \sleep(1);
            $expected_value = $item;
        }
        $result = $distinctSeriesValue->get($key);
        unset($result['last_update']);
        unset($result['value_created']);

        $this->assertEquals(array('value' => $expected_value), $result);
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
        $testGroupName = 'DistinctSeriesValueCreate';
        $seriesValue = DistinctSeries::create($testGroupName, self::$connection);
        $resultGroupName = $seriesValue->getGrouping();
        $this->assertEquals($testGroupName, $resultGroupName);
        $this->assertInstanceOf('RyanWHowe\KeyValueStore\KeyValue\DistinctSeries', $seriesValue);
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
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::set
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::update
     * @dataProvider nonUniqueKeyDataProvider
     * @throws \Exception
     */
    public function uniqueKeysCheck($testSet)
    {
        $testGroup = 'DistinctSeriesUniqueKeys';
        $distinctSeries = DistinctSeries::create($testGroup, self::$connection);
        $expected = array();
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $distinctSeries->set($key, $value);
            }
            $expected[\strtolower($key)] = true;
            $result = $distinctSeries->getAllKeys();
            $this->assertEquals(array_keys($expected), $result);
        }

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
     * @dataProvider groupingTestProvider
     */
    public function getAllKeysFalseCheck($testGroup, $expectedGroup, $expectedResult)
    {
        $distinctSeries = DistinctSeries::create($testGroup, self::$connection);
        $result = $distinctSeries->getAllKeys();
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
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::set
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::update
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\DistinctSeries::getLastUnique
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\Multi::get
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\Multi::getSeriesCreateDate
     * @dataProvider setGetDataProvider
     * @param string $key
     * @param array $values
     * @throws \Exception
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getLastUniqueCheck($key, $values)
    {
        $testGrouping = 'DistinctSeriesValueGet';
        $expected_value = '';
        $value = array();
        $distinctSeriesValue = DistinctSeries::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $distinctSeriesValue->set($key, $item);
            /* The sleep is needed to have the sqlite database see a difference in timestamp values*/
            \sleep(1);
            if ( ! array_key_exists($item, $value)) {
                /* the last set distinct value needs to be captured */
                $value[$item] = true;
            }
        }
        $result = $distinctSeriesValue->getLastUnique($key);
        unset($result['last_update']);
        unset($result['value_created']);
        foreach ($value as $set_value => $item) {
            $expected_value = $set_value;
        }

        $this->assertEquals(array('value' => $expected_value), $result);
    }
}
