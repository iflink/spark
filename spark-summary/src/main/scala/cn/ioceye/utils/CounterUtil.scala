package cn.ioceye.utils

import java.lang.{Float => JFloat}

import org.apache.spark.sql.Row

object CounterUtil {
  def getString(row: Row, index: Int): String = {
    if (row.isNullAt(index)) {
      null
    }
    else {
      val bytes: Array[Byte] = row.getAs[Array[Byte]](index)
      new String(bytes)
    }
  }

  def getStringToFloat(row: Row, index: Int): JFloat = {
    val value: String = getString(row, index)
    if (value == null) {
      null
    }
    else {
      value.toFloat
    }
  }

  def convertBytesToInt(row: Row, index: Int): Integer = {
    if (row.isNullAt(index)) {
      null
    }
    else {
      val bytes: Array[Byte] = row.getAs[Array[Byte]](index)
      new String(bytes).toInt
    }
  }

  def convertToInt(row: Row, index: Int): Integer = {
    if (row.isNullAt(index)) {
      null
    }
    else {
      row.getInt(index)
    }
  }

  def convertBytesToFloat(row: Row, index: Int): JFloat = {
    if (row.isNullAt(index)) {
      null
    }
    else {
      val bytes: Array[Byte] = row.getAs[Array[Byte]](index)
      new String(bytes).toFloat
    }
  }

  def convertToFloat(row: Row, index: Int): JFloat = {
    if (row.isNullAt(index)) {
      null
    }
    else {
      row.getFloat(index)
    }
  }

  def getIf(a: JFloat, b: Int): Integer = {
    if (a == null) {
      null
    } else {
      b
    }
  }

  def multi(a: JFloat, b: Integer): JFloat = {
    if (a == null || b == null) {
      null
    }
    else {
      a * b
    }
  }

  def multi(a: JFloat, b: Int): JFloat = {
    if (a == null) {
      null
    }
    else {
      a * b
    }
  }

  def add(a: JFloat, b: JFloat): JFloat = {
    if (a == null) {
      b
    }
    else if (b == null){
      a
    }
    else {
      a + b
    }
  }

  def add(a: Integer, b: Integer): Integer = {
    if (a == null) {
      b
    }
    else if (b == null){
      a
    }
    else {
      a + b
    }
  }
}
