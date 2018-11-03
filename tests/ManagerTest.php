<?php
/**
 * This file contains
 *
 * @author Ryan Howe
 * @since  2018-10-12
 */

namespace Test;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use RyanWHowe\KeyValueStore\Manager;

class ManagerTest extends \PHPUnit\Framework\TestCase {

    /**
     * @var \Doctrine\DBAL\Connection
     */
    protected static $connection;

    /**
     * @var array The local SQLite memory database connection configuration array
     */
    protected static $databaseConfig = array(
        'dbname' => ':memory:',
        'host'   => 'localhost',
        'driver' => 'pdo_sqlite',
    );

    /**
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function setUpBeforeClass()
    {
        parent::setUpBeforeClass();
        self::$connection = DriverManager::getConnection(self::$databaseConfig, new Configuration());
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     */
    public function testCreate()
    {
        $test = Manager::create(self::$connection);
        $this->assertInstanceOf('RyanWHowe\KeyValueStore\Manager', $test);
    }

    /**
     * @test
     * @throws \Doctrine\DBAL\DBALException
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     */
    public function testCreateTable()
    {
        $test = Manager::create(self::$connection);
        $test->createTable();
        $sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='ValueStore';";
        $result = self::$connection->executeQuery($sql)->fetch(\PDO::FETCH_COLUMN);
        $test->dropTable();
        $this->assertEquals('ValueStore', $result);
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @covers \RyanWHowe\KeyValueStore\Manager::getAllGroupings
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::create
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue\Single::set
     * @covers \RyanWHowe\KeyValueStore\KeyValue::insert
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getId
     * @covers \RyanWHowe\KeyValueStore\KeyValue::formatGrouping
     * @covers \RyanWHowe\KeyValueStore\KeyValue::__construct
     * @covers \RyanWHowe\KeyValueStore\KeyValue::getGrouping
     * @throws \Exception
     */
    public function testGetAllGroupings()
    {
        $test = Manager::create(self::$connection);
        $test->createTable();
        $expected = array();
        $singleValue = \RyanWHowe\KeyValueStore\KeyValue\Single::create('Test1', self::$connection);
        $expected[] = 'Test1';
        $singleValue->set('Key', 'Value');
        $singleValue = \RyanWHowe\KeyValueStore\KeyValue\Single::create('Test2', self::$connection);
        $expected[] = 'Test2';
        $singleValue->set('Key', 'Value');
        $singleValue = \RyanWHowe\KeyValueStore\KeyValue\Single::create('Test3', self::$connection);
        $expected[] = 'Test3';
        $singleValue->set('Key', 'Value');
        $result = $test->getAllGroupings();
        $test->dropTable();
        $this->assertEquals($expected, $result);
    }

    /**
     * @test
     * @covers \RyanWHowe\KeyValueStore\Manager::create
     * @covers \RyanWHowe\KeyValueStore\Manager::__construct
     * @covers \RyanWHowe\KeyValueStore\Manager::createTable
     * @covers \RyanWHowe\KeyValueStore\Manager::dropTable
     * @throws \Doctrine\DBAL\DBALException
     */
    public function testDropTable()
    {
        $test = Manager::create(self::$connection);
        $test->createTable();
        $sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='ValueStore';";
        $test->dropTable();
        $result = self::$connection->executeQuery($sql)->fetch(\PDO::FETCH_COLUMN);
        $this->assertEquals(false, $result);
    }

}
