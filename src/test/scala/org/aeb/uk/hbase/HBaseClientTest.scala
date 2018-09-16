package org.aeb.uk.hbase

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, QualifierFilter, ValueFilter}
import org.apache.logging.log4j.scala.Logging
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

class HBaseClientTest extends FlatSpec with BeforeAndAfterEach with Logging {

  val hBaseSiteXmlPath = "src/test/resources/docker/dockerfiles/conf/hbase-site.xml"

  val hBaseClient : HBaseClient = new HBaseClient( hBaseSiteXmlPath )

  override def beforeEach(): Unit = {
    hBaseClient.connect
  }

  override def afterEach(): Unit = {
    hBaseClient.close()
  }

  behavior of "HBaseClient"

  it should "create a new table with a specified schema" in {

    val tableName = "test_table_1"
    val columnFamilyNames = Array( "column_family_1", "column_family_2", "column_family_3" )

    hBaseClient.createTable( tableName, columnFamilyNames )

    // Todo: automate the schema check

  }

  it should "delete an existing table" in {

    val tableName = "test_table_2"
    val columnFamilyNames = Array( "column_family_1", "column_family_2", "column_family_3" )

    hBaseClient.createTable( tableName, columnFamilyNames )
    hBaseClient.deleteTable( tableName )

    // Todo: automate the check on the deletion of the table

  }

  it should "overwrite an existing table" in {

    val tableName = "test_table_3"
    val initialColumnFamilyNames = Array( "column_family_1", "column_family_2", "column_family_3" )

    hBaseClient.createTable( tableName, initialColumnFamilyNames )

    val finalColumnFamilyNames = Array( "new_column_family_1", "new_column_family_2" )

    hBaseClient.createTable( tableName, finalColumnFamilyNames )

    // Todo: automate the schema check

  }

  it should "put and get a single row value from an HBase table" in {

    val tableName = "test_table_4"
    val columnFamilyNames = Array( "column_family" )

    val hBaseRow = HBaseRow( "0001", columnFamilyNames(0), "first_name", "Adam" )

    hBaseClient.createTable( tableName, columnFamilyNames )

    hBaseClient.put( hBaseRow, tableName )

    val rowValue = hBaseClient.get( hBaseRow.key, tableName )

    assert( rowValue === hBaseRow.value )

  }

  it should "put multiple values into a single row and retrieve the last value from an HBase table" in {

    val tableName = "test_table_5"
    val rowKey = "0001"
    val columnFamilyNames = Array( "column_family" )

    val hBaseRowFirstName = HBaseRow( rowKey, columnFamilyNames(0), "first_name", "Adam" )
    val hBaseRowFamilyName = HBaseRow( rowKey, columnFamilyNames(0), "family_name", "Brook" )
    val hBaseRowAge = HBaseRow( rowKey, columnFamilyNames(0), "age", "37" )

    hBaseClient.createTable( tableName, columnFamilyNames )

    hBaseClient.put( hBaseRowFirstName, tableName )
    hBaseClient.put( hBaseRowFamilyName, tableName )
    hBaseClient.put( hBaseRowAge, tableName )

    val rowValue = hBaseClient.get( rowKey, tableName )

    assert( rowValue === "37" )

  }

  it should "return a scan of the current table contents" in {

    val tableName = "test_table_6"
    val columnFamilyNames = Array( "column_family" )
    val qualifier = "first_name"

    val hBaseRow1 = HBaseRow( "0001", columnFamilyNames(0), "first_name", "Adam" )
    val hBaseRow2 = HBaseRow( "0002", columnFamilyNames(0), "first_name", "Bridget" )
    val hBaseRow3 = HBaseRow( "0003", columnFamilyNames(0), "first_name", "Charles" )

    hBaseClient.createTable( tableName, columnFamilyNames )

    hBaseClient.put( hBaseRow1, tableName )
    hBaseClient.put( hBaseRow2, tableName )
    hBaseClient.put( hBaseRow3, tableName )

    val resultScan = hBaseClient.scan( tableName, columnFamilyNames(0), qualifier )

    // Todo: relies on ordering, should be changed so it tests on all in any order
    assert( new String( resultScan.next().getValue( columnFamilyNames(0).getBytes, qualifier.getBytes ) ) === hBaseRow1.value )
    assert( new String( resultScan.next().getValue( columnFamilyNames(0).getBytes, qualifier.getBytes ) ) === hBaseRow2.value )
    assert( new String( resultScan.next().getValue( columnFamilyNames(0).getBytes, qualifier.getBytes ) ) === hBaseRow3.value )

  }

  it should "return a scan of the current table contents filtering on a qualifier and value" in {

    val tableName = "test_table_7"
    val ingestionQualifier = "converted"
    val columnFamilyNames = Array( "column_family" )
    val qualifier = "ingested"

    val hBaseRow1 = HBaseRow( "0001", columnFamilyNames(0), qualifier, "true" )
    val hBaseRow2 = HBaseRow( "0002", columnFamilyNames(0), qualifier, "false" )
    val hBaseRow3 = HBaseRow( "0003", columnFamilyNames(0), qualifier, "true" )
    val hBaseRow4 = HBaseRow( "0003", columnFamilyNames(0), "conversion", "true" )

      hBaseClient.createTable( tableName, columnFamilyNames )

    hBaseClient.put( hBaseRow1, tableName )
    hBaseClient.put( hBaseRow2, tableName )
    hBaseClient.put( hBaseRow3, tableName )
    hBaseClient.put( hBaseRow4, tableName )

    val ingestionQualifierFilter = new QualifierFilter(
      CompareOp.GREATER_OR_EQUAL,
      new BinaryComparator( ingestionQualifier.getBytes )
    )
    val booleanValueFilter = new ValueFilter(
      CompareOp.EQUAL,
      new BinaryComparator( "true".getBytes )
    )

    val filterForIngestionRows = List( ingestionQualifierFilter, booleanValueFilter )

    val resultScan = hBaseClient.scanFilter( tableName, filterForIngestionRows )

    assert( new String( resultScan.next().getValue( columnFamilyNames(0).getBytes, qualifier.getBytes ) ) === hBaseRow1.value )
    assert( new String( resultScan.next().getValue( columnFamilyNames(0).getBytes, qualifier.getBytes ) ) === hBaseRow3.value )
    assert( resultScan.next() === null )

  }

}