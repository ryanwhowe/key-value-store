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
     * Testing the getGrouping method
     *
     * @param string $testGroup      The provided Test Group Name
     * @param string $expectedGroup  The expected Result of the output
     * @param string $expectedResult The expected assertion result
     *
     * @test
     *
     * @covers       \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers       \RyanWHowe\KeyValueStore\Manager::create
     * @covers       \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers       \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers       \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     *
     * @dataProvider groupingTestProvider
     *
     * @throws \Exception
     *
     * @return void
     */
    public function getGrouping($testGroup, $expectedGroup, $expectedResult)
    {
        $distinctSeries = DistinctSeries::create($testGroup, self::$connection);
        $resultGroup = $distinctSeries->getGrouping();
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
     * @throws \Exception
     */
    public function delete()
    {
        $testGroup = 'SeriesValueDelete';
        $key = 'KeyValue';
        $values = array(
            'value1',
            'value2',
            'value3',
            'value3',
            'value2',
            'value2',
            'value1'
        );
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
     * @throws \Exception
     */
    public function getGroupingSet($testSet)
    {
        $testGroup = 'DistinctSeriesValueGetGroupingSet';

        $distinctSeries = DistinctSeries::create($testGroup, self::$connection);
        $expected = array();
        foreach ($testSet as $item) {
            $key = $item['key'];
            foreach ($item['values'] as $value) {
                $distinctSeries->set($key, $value);
                \usleep(1000);
                $expectedValue = $value; // we expect the last value set
            }

            $expected[] = array('key' => \strtolower($key), 'value' => $expectedValue);
        }

        $result = $distinctSeries->getGroupingSet();

        foreach ($result as &$item) {
            // We are removing the last_update, this is a timestamp and is not testable
            unset($item['last_update']);
            unset($item['value_created']);
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
     */
    public function get($key, $values)
    {
        $testGrouping = 'DistinctSeriesValueGet';
        $expectedValue = '';

        $distinctSeries = DistinctSeries::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $distinctSeries->set($key, $item);
            /* The sleep is needed to have the sqlite database see a difference in timestamp values*/
            \usleep(1000);
            $expectedValue = $item;
        }
        $result = $distinctSeries->get($key);
        unset($result['last_update']);
        unset($result['value_created']);

        $this->assertEquals(array('value' => $expectedValue), $result);
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
     */
    public function getSet($testSet)
    {
        $testGrouping = 'DistinctSeriesValueGetSet';

        $distinctSeries = DistinctSeries::create($testGrouping, self::$connection);

        foreach ($testSet as $test) {
            $expected = array();
            $expectedValues = array();
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $distinctSeries->set($key, $value);
                if ( ! array_key_exists($value, $expectedValues)) {
                    $expectedValues[$value] = true;
                }
            }
            foreach ($expectedValues as $values => $item) {
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
        $expectedValue = '';
        $distinctSeries = DistinctSeries::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $distinctSeries->set($key, $item);
            /* The sleep is needed to have the sqlite database see a difference in timestamp values*/
            \usleep(1000);
            $expectedValue = $item;
        }
        $result = $distinctSeries->get($key);
        unset($result['last_update']);
        unset($result['value_created']);

        $this->assertEquals(array('value' => $expectedValue), $result);
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
        $distinctSeries = DistinctSeries::create($testGroupName, self::$connection);
        $resultGroupName = $distinctSeries->getGrouping();
        $this->assertEquals($testGroupName, $resultGroupName);
        $this->assertInstanceOf('RyanWHowe\KeyValueStore\KeyValue\DistinctSeries',
            $distinctSeries);
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
     * @param $testSet
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
     * @param $testGroup
     * @throws \Exception
     */
    public function getAllKeysFalseCheck($testGroup)
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
     */
    public function getLastUniqueCheck($key, $values)
    {
        $testGrouping = 'DistinctSeriesValueGet';
        $expectedValue = '';
        $value = array();
        $distinctSeries = DistinctSeries::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $distinctSeries->set($key, $item);
            /* The sleep is needed to have the sqlite database see a difference in timestamp values*/
            \usleep(1000);
            if ( ! array_key_exists($item, $value)) {
                /* the last set distinct value needs to be captured */
                $value[$item] = true;
            }
        }
        $result = $distinctSeries->getLastUnique($key);
        unset($result['last_update']);
        unset($result['value_created']);
        foreach ($value as $setValue => $item) {
            $expectedValue = $setValue;
        }

        $this->assertEquals(array('value' => $expectedValue), $result);
    }
}
