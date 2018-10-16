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
     * @throws \Doctrine\DBAL\DBALException
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
     * @dataProvider multiKeyDataProvider
     * @param array $testSet
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function getGroupingSet($testSet)
    {
        $testGroup = 'SingleValueGetGroupingSet';
        $singleValue = Single::create($testGroup, self::$connection);
        $expected = array();
        $expected_value = '';
        foreach ($testSet as $test) {
            $key = $test['key'];
            foreach ($test['values'] as $value) {
                $singleValue->set($key, $value);
                $expected_value = $value; // we expect the last value set
            }
            $expected[] = array('grouping' => $testGroup, 'key' => \strtolower($key), 'value' => $expected_value);
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
     * @throws \Exception
     */
    public function create()
    {
        $testGrouping = 'SingleValueCreate';
        $singleValue = Single::create($testGrouping, self::$connection);
        $resultGrouping = $singleValue->getGrouping();
        $this->assertEquals($testGrouping, $resultGrouping);
        $this->assertInstanceOf('RyanWHowe\KeyValueStore\KeyValue\Single', $singleValue);
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
     * @throws \Doctrine\DBAL\DBALException
     */
    public function set($key, array $values)
    {
        $testGroup = 'SingleValueSet';
        $expected = '';
        $singleValue = Single::create($testGroup, self::$connection);
        foreach ($values as $value) {
            $singleValue->set($key, $value);
            $expected = $value;  // the last set value is what we expect out
        }

        $result = $singleValue->get($key);

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
     * @dataProvider groupingTestProvider
     * @throws \Exception
     */
    public function GetGrouping($testGroup, $expectedGroup, $expectedResult)
    {
        $singleValue = Single::create($testGroup, self::$connection);
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
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function get($key, array $values)
    {
        $testGroup = 'SingleValueGet';
        $expected = '';
        $singleValue = Single::create($testGroup, self::$connection);
        foreach ($values as $value) {
            $singleValue->set($key, $value);
            $expected = $value;  // the last set value is what we expect out
        }

        $result = $singleValue->get($key);

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
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function delete()
    {
        $testGroup = 'SingleValueDelete';
        $key = 'KeyValue';
        $singleValue = Single::create($testGroup, self::$connection);
        $singleValue->set($key, 'value');
        $singleValue->delete($key);
        $result = $singleValue->get($key);
        $this->assertFalse($result);
    }

    /**
     * @test
     * @dataProvider nonUniqueKeyDataProvider
     * @throws \Exception
     */
    public function uniqueKeys($testSet)
    {
        $testGroup = 'SingleUniqueKeys';
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
}
