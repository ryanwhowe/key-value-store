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
     * Test for the set function
     *
     * @param string $key    The key value that will be utilized
     * @param array  $values The value stored with the corresponding key
     *
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
     *
     * @dataProvider setGetDataProvider
     *
     * @throws \Exception
     * @return void
     */
    public function set($key, $values)
    {
        $testGrouping = 'SeriesValueGet';
        $value = '';
        $series = Series::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $series->set($key, $item);
            /*
            The sleep is needed to have the sqlite database see a difference in
            timestamp values
            */
            \usleep(1000);
            $value = $item; //the expected output is the last value that was set
        }
        $result = $series->get($key);
        unset($result['last_update']);
        unset($result['value_created']);
        $this->assertEquals(array('value' => $value), $result);
    }

    /**
     * Test for the getSet funciton
     *
     * @param array $testSet The testset that will be used
     *
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
     *
     * @dataProvider multiKeyDataProvider
     *
     * @throws \Exception
     *
     * @return void
     */
    public function getSet($testSet)
    {
        $testGrouping = 'SeriesValueGetSet';
        $series = Series::create($testGrouping, self::$connection);

        foreach ($testSet as $test) {
            $expected = array();
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $series->set($key, $value);
                $expected[] = array('value' => $value);
            }

            $result = $series->getSet($key);
            foreach ($result as &$item) {
                unset($item['last_update']);
                unset($item['value_created']);
            }
            $this->assertEquals($expected, $result);
        }
    }

    /**
     * Test for the getAllKeys function
     *
     * @param array $testSet The testset array used
     *
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
     *
     * @dataProvider multiKeyDataProvider
     *
     * @throws \Exception
     *
     * @return void
     */
    public function getAllKeys($testSet)
    {
        $series = Series::create('SeriesValueGetAllKeys', self::$connection);
        $expected = array();
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $series->set($key, $value);
            }
            $expected[] = \strtolower($key);
            $result = $series->getAllKeys();
            $this->assertEquals($expected, $result);
        }
        $result = $series->getAllKeys();
        $this->assertEquals($expected, $result);
    }

    /**
     * The test for the create method
     *
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     *
     * @throws \Exception
     *
     * @return void
     */
    public function create()
    {
        $testGroupName = 'SeriesValueCreate';
        $series = Series::create($testGroupName, self::$connection);
        $resultGroupName = $series->getGrouping();
        $this->assertEquals($testGroupName, $resultGroupName);
        $this->assertInstanceOf(
            'RyanWHowe\KeyValueStore\KeyValue\Series',
            $series
        );
    }

    /**
     * Test the getGroupingSet function
     *
     * @param array $testSet
     *
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
     *
     * @dataProvider multiKeyDataProvider
     *
     * @throws \Exception
     *
     * @return void
     */
    public function getGroupingSet($testSet)
    {
        $testGroup = 'SeriesValueGetGroupingSet';
        $series = Series::create($testGroup, self::$connection);
        $expected = array();
        $expectedValue = '';
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $series->set($key, $value);
                $expectedValue = $value; // we expect the last value set
            }
            $expected[] = array(
                'key'   => \strtolower($key),
                'value' => $expectedValue
            );
        }

        $result = $series->getGroupingSet();

        foreach ($result as &$item) {
            // We are removing the last_update, the timestamp and is not testable
            unset($item['last_update']);
            unset($item['value_created']);
        }

        $this->assertEquals($expected, $result);
    }

    /**
     * Test the get method
     *
     * @param string $key    The key to associated the value to
     * @param array  $values The value to be associated with the key
     *
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
     *
     * @dataProvider setGetDataProvider
     *
     * @throws \Exception
     *
     * @return void
     */
    public function get($key, $values)
    {
        $testGrouping = 'SeriesValueGet';
        $value = '';
        $series = Series::create($testGrouping, self::$connection);
        foreach ($values as $item) {
            $series->set($key, $item);
            /*
            The sleep is needed to have the sqlite database see a difference in
            timestamp values
            */
            \usleep(1000);
            $value = $item; //the expected output is the last value that was set
        }
        $result = $series->get($key);
        unset($result['last_update']);
        unset($result['value_created']);
        $this->assertEquals(array('value' => $value), $result);
    }

    /**
     * Test the getGrouping method
     *
     * @param string  $testGroup      The test group name input
     * @param string  $expectedGroup  The test group output name
     * @param boolean $expectedResult The test group expected result
     *
     * @test
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
        $series = Series::create($testGroup, self::$connection);
        $resultGroup = $series->getGrouping();
        if ($expectedResult) {
            $this->assertEquals($expectedGroup, $resultGroup);
        } else {
            $this->assertNotEquals($expectedGroup, $resultGroup);
        }
    }

    /**
     * The delete method test
     *
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
     *
     * @throws \Exception
     *
     * @return void
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
        $series = Series::create($testGroup, self::$connection);
        foreach ($values as $value) {
            $series->set($key, $value);
        }
        $series->delete($key);
        $result = $series->get($key);
        $this->assertFalse($result);
    }

    /**
     * The unique keys check
     *
     * @param array $testSet The test array to process though
     *
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
     *
     * @dataProvider nonUniqueKeyDataProvider
     *
     * @throws \Exception
     *
     * @return void
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

    /**
     * The getAllKeysFalseCheck method test
     *
     * @param string $testGroup the grouping name to test
     *
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
     *
     * @dataProvider groupingTestProvider
     *
     * @throws \Exception
     *
     * @return void
     */
    public function getAllKeysFalseCheck($testGroup)
    {
        $series = Series::create($testGroup, self::$connection);
        $result = $series->getAllKeys();
        $this->assertFalse($result);
    }
}
