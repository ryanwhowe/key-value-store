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
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function getAllKeys()
    {
        $test = Single::create('SingleValueGetAllKeys', self::$connection);
        $test->set('key1', 'value1');
        $test->set('key1', 'value2');
        $test->set('key2', 'value2');
        $test->set('key2', 'value3');
        $test->set('key3', 'value3');
        $test->set('key3', 'value4');
        $test->set('key4', 'value4');
        $test->set('key4', 'value5');
        $test->set('key5', 'value5');
        $test->set('key5', 'value6');
        $expected = array('key1', 'key2', 'key3', 'key4', 'key5');
        $result = $test->getAllKeys();
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
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function getGroupingSet()
    {
        $testGroup = 'SingleValueGetGroupingSet';

        $expected = array(
            array('grouping' => $testGroup, 'key' => 'key1', 'value' => 'value1'),
            array('grouping' => $testGroup, 'key' => 'key2', 'value' => 'value2'),
            array('grouping' => $testGroup, 'key' => 'key3', 'value' => 'value3'),
            array('grouping' => $testGroup, 'key' => 'key4', 'value' => 'value4'),
            array('grouping' => $testGroup, 'key' => 'key5', 'value' => 'value5'),
            array('grouping' => $testGroup, 'key' => 'key6', 'value' => 'value6')
        );

        $singleValue = Single::create($testGroup, self::$connection);
        foreach ($expected as $item) {
            $singleValue->set($item['key'], $item['value']);
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
     * @throws \Exception
     * @throws \Doctrine\DBAL\DBALException
     */
    public function set()
    {
        $testGroup = 'SingleValueSet';
        $key = 'Key1';
        $expected = '';
        $testValues = array(
            'value1',
            'value2',
            'value3',
            'value4',
            'value5',
            'value6',
            'value3'
        );

        $singleValue = Single::create($testGroup, self::$connection);
        foreach ($testValues as $value) {
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
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function get()
    {
        $testGroup = 'SingleValueGet';
        $key = 'Key1';
        $expected = '';
        $testValues = array(
            'value1',
            'value2',
            'value3',
            'value4',
            'value5',
            'value6',
            'value3'
        );

        $singleValue = Single::create($testGroup, self::$connection);
        foreach ($testValues as $value) {
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
}
