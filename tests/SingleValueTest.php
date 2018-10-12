<?php
/**
 * This file contains
 *
 * @author Ryan Howe
 * @since  2018-10-12
 */

namespace Test;

use ryanwhowe\KeyValueStore\Store\SingleValue;

class SingleValueTest extends DataTransaction {

    /**
     * @test
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Exception
     */
    public function getAllKeys()
    {
        $test = SingleValue::create('SingleValueGetAllKeys', self::$connection);
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

        $singleValue = SingleValue::create($testGroup, self::$connection);
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
     * @throws \Exception
     */
    public function create()
    {
        $testGrouping = 'SingleValueCreate';
        $singleValue = SingleValue::create($testGrouping, self::$connection);
        $resultGrouping = $singleValue->getGrouping();
        $this->assertEquals($testGrouping, $resultGrouping);
        $this->assertInstanceOf('ryanwhowe\KeyValueStore\Store\SingleValue', $singleValue);
    }

    /**
     * @test
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

        $singleValue = SingleValue::create($testGroup, self::$connection);
        foreach ($testValues as $value) {
            $singleValue->set($key, $value);
            $expected = $value;  // the last set value is what we expect out
        }

        $result = $singleValue->get($key);

        $this->assertEquals($expected, $result);
    }

    public function groupingTestProvider()
    {
        return array(
            array('GroupName', 'GroupName', true),
            array('GroupName1', 'GroupName1', true),
            array('Group Name', 'Group_Name', true),
            array('G r o u p N a m e ', 'G_r_o_u_p_N_a_m_e', true),
            array(' GroupName', 'GroupName', true),
            array(' GroupName ', 'GroupName', true),
            array('GroupName 12', 'GroupName_12', true),
            array(' G r o u p N a m e 1 2 ', 'G_r_o_u_p_N_a_m_e_1_2', true),
            array('GroupName', 'GroupName', true),

            array(' GroupName', ' GroupName', false),
            array('GroupName1 ', 'GroupName1 ', false),
            array('Group Name', 'Group Name', false),
            array('G r o u p N a m e ', 'G r o u p N a m e ', false),
            array(' GroupName', ' GroupName', false),
            array(' GroupName ', ' GroupName ', false),
            array('GroupName 12', 'GroupName 12', false),
            array(' G r o u p N a m e 1 2 ', ' G r o u p N a m e 1 2 ', false),
            array('G r o u p N a m e ', 'G r o u p N a m e ', false),
        );
    }

    /**
     * @test
     * @dataProvider groupingTestProvider
     * @throws \Exception
     */
    public function GetGrouping($testGroup, $expectedGroup, $expectedResult)
    {
        $singleValue = SingleValue::create($testGroup, self::$connection);
        $resultGroup = $singleValue->getGrouping();
        if ($expectedResult) {
            $this->assertEquals($expectedGroup, $resultGroup);
        } else {
            $this->assertNotEquals($expectedGroup, $resultGroup);
        }
    }

    /**
     * @test
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

        $singleValue = SingleValue::create($testGroup, self::$connection);
        foreach ($testValues as $value) {
            $singleValue->set($key, $value);
            $expected = $value;  // the last set value is what we expect out
        }

        $result = $singleValue->get($key);

        $this->assertEquals($expected, $result);
    }

    /**
     * @test
     * @throws \Doctrine\DBAL\DBALException
     */
    public function delete()
    {
        $testGroup = 'SingleValueDelete';
        $key = 'KeyValue';
        $singleValue = SingleValue::create($testGroup, self::$connection);
        $singleValue->set($key, 'value');
        $singleValue->delete($key);
        $result = $singleValue->get($key);
        $this->assertFalse($result);
    }
}
