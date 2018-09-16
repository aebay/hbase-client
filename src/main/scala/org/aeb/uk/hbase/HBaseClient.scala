package org.aeb.uk.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter, FilterList}
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.scala.Logging

class HBaseClient( hBaseSiteXmlPath: String ) extends Logging {

  var configuration: Configuration = _
  var connection: Connection = _
  var admin: Admin = _

  def connect: Unit = {

    configuration = HBaseConfiguration.create()

    configuration.addResource( hBaseSiteXmlPath )

    HBaseAdmin.checkHBaseAvailable( configuration )

    connection = ConnectionFactory.createConnection( configuration )

    admin = connection.getAdmin

  }

  private def createOrOverwriteTable( tableDescriptor: HTableDescriptor ): Unit = {

    deleteTable( tableDescriptor )

    admin.createTable( tableDescriptor )

  }

  def createTable( tableName: String, columnFamilies: Array[String] ): Unit = {

    val table = new HTableDescriptor( TableName.valueOf( tableName ) )

    for ( columnFamily <- columnFamilies ) table.addFamily( new HColumnDescriptor( columnFamily ) )

    createOrOverwriteTable( table )

  }

  def deleteTable( tableName: String ): Unit = {

    val tableDescriptor = new HTableDescriptor( TableName.valueOf( tableName ) )

    deleteTable( tableDescriptor )

  }

  private def deleteTable( tableDescriptor: HTableDescriptor ): Unit = {

    val tableName = tableDescriptor.getTableName

    if ( admin.tableExists( tableName ) ) {
      admin.disableTable( tableName )
      admin.deleteTable( tableName )
    }

  }

  def get( hBaseRowKey: String, tableName: String ): String = {

    val rowKey = new Get( Bytes.toBytes( hBaseRowKey ) )

    val table: HTable = new HTable( configuration, tableName )

    Bytes.toString( table.get( rowKey ).value() )

  }

  def put( hBaseRow: HBaseRow, tableName: String ): Unit = {

    val row = new Put( new String( hBaseRow.key ).getBytes )
    row.addImmutable( hBaseRow.columnFamily.getBytes(), hBaseRow.qualifier.getBytes(), hBaseRow.value.getBytes() )

    val table: HTable = new HTable( configuration, tableName )

    table.put( row )

  }

  def scan( tableName: String, columnFamily: String, qualifier: String ): ResultScanner = {

    val scan: Scan = new Scan()
    scan.addColumn( columnFamily.getBytes, qualifier.getBytes )

    val table: HTable = new HTable( configuration, tableName )

    table.getScanner( scan )

  }

  def scanFilter( tableName: String, filters: List[Filter] ): ResultScanner = { // Todo: convert

    import scala.collection.JavaConverters._

    val scan: Scan = new Scan()
    scan.setFilter( new FilterList( Operator.MUST_PASS_ALL, filters.asJava ) )

    val table: HTable = new HTable( configuration, tableName )

    table.getScanner( scan )

  }

  def close(): Unit = {
    connection.close()
  }



}
