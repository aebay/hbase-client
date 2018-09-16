package org.aeb.uk.hbase

case class HBaseRow( key: String, columnFamily: String, qualifier: String, value: String )
