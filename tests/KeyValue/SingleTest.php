<?php
/**
 * This file contains
 *
 * @author Ryan Howe
 * @since  2018-10-12
 */

namespace Test\KeyValue;

use RyanWHowe\KeyValueStore\KeyValue\Single;

class SingleTest extends DataTransaction {

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::getAllKeys
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::update
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @dataProvider multiKeyDataProvider
     * @param array $testSet
     * @throws \Exception
     */
    public function getAllKeys(array $testSet)
    {
        $single = Single::create('SingleValueGetAllKeys', self::$connection);
        $expected = array();
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $single->set($key, $value);
            }
            $expected[] = \strtolower($key);
            $result = $single->getAllKeys();
            $this->assertEquals($expected, $result);
        }
        $result = $single->getAllKeys();
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
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGroupingSet
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::update
     * @dataProvider multiKeyDataProvider
     * @param array $testSet
     * @throws \Exception
     */
    public function getGroupingSet($testSet)
    {
        $testGroup = 'SingleValueGetGroupingSet';
        $single = Single::create($testGroup, self::$connection);
        $expected = array();
        $expectedValue = '';
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $single->set($key, $value);
                $expectedValue = $value; // we expect the last value set
            }
            $expected[] = array('key' => \strtolower($key), 'value' => $expectedValue);
        }

        $result = $single->getGroupingSet();

        foreach ($result as &$item) {
            // We are removing the times, this is a timestamp and is not testable
            unset($item['value_created']);
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
     * @throws \Exception
     */
    public function create()
    {
        $testGrouping = 'SingleValueCreate';
        $single = Single::create($testGrouping, self::$connection);
        $resultGrouping = $single->getGrouping();
        $this->assertEquals($testGrouping, $resultGrouping);
        $this->assertInstanceOf('RyanWHowe\KeyValueStore\KeyValue\Single', $single);
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
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::get
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::update
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::get
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::update
     * @dataProvider setGetDataProvider
     * @param string $key
     * @param array $values
     * @throws \Exception
     */
    public function set($key, array $values)
    {
        $testGroup = 'SingleValueSet';
        $expected = '';
        $single = Single::create($testGroup, self::$connection);
        foreach ($values as $value) {
            $single->set($key, $value);
            $expected = array('value' => $value);  // the last set value is what we expect out
        }

        $result = $single->get($key);

        unset($result['value_created']);
        unset($result['last_update']);

        $this->assertEquals($expected, $result);
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
     * @dataProvider groupingTestProvider
     * @param $testGroup
     * @param $expectedGroup
     * @param $expectedResult
     * @throws \Exception
     */
    public function getGrouping($testGroup, $expectedGroup, $expectedResult)
    {
        $single = Single::create($testGroup, self::$connection);
        $resultGroup = $single->getGrouping();
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
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::get
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::update
     * @dataProvider setGetDataProvider
     * @param string $key
     * @param array $values
     * @throws \Exception
     */
    public function get($key, array $values)
    {
        $testGroup = 'SingleValueGet';
        $expected = '';
        $single = Single::create($testGroup, self::$connection);
        foreach ($values as $value) {
            $single->set($key, $value);
            $expected = array('value' => $value);  // the last set value is what we expect out
        }

        $result = $single->get($key);

        unset($result['value_created']);
        unset($result['last_update']);


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
     * @covers \RyanWHowe\KeyValueStore\KeyValue::delete
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::get
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::set
     * @throws \Exception
     */
    public function delete()
    {
        $testGroup = 'SingleValueDelete';
        $key = 'KeyValue';
        $single = Single::create($testGroup, self::$connection);
        $single->set($key, 'value');
        $single->delete($key);
        $result = $single->get($key);
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
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\Single::set
     * @covers       \RyanWHowe\KeyValueStore\KeyValue\Single::update
     * @dataProvider nonUniqueKeyDataProvider
     * @param $testSet
     * @throws \Exception
     */
    public function uniqueKeysCheck($testSet)
    {
        $testGroup = 'SeriesUniqueKeys';
        $single = Single::create($testGroup, self::$connection);
        $expected = array();
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $single->set($key, $value);
            }
            $expected[\strtolower($key)] = true;
            $result = $single->getAllKeys();
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
        $single = Single::create($testGroup, self::$connection);
        $result = $single->getAllKeys();
        $this->assertFalse($result);
    }
}
