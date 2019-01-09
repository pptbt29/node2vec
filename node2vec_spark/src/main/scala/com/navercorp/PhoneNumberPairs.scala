package com.navercorp

import org.apache.spark.sql.SparkSession

class PhoneNumberPairs(
                        val spark: SparkSession,
                        var i_user_contact_start_date: String = "",
                        var i_user_contact_end_date: String = "",
                        var a_user_table_date: String = "",
                        var data_size_limit: String = "",
                        var idsOfSelectedRegions: Array[String] = null
                      ) {
  import org.apache.spark.sql.{DataFrame, SparkSession, Row}
  import spark.implicits._

  var pnp: DataFrame = _
  var distinctPhoneNumbers: DataFrame = _
  var phoneCount: Long = _
  var edgeCount: Long = _
  var outDegree: DataFrame = _
  var inDegree: DataFrame = _

  def getPhoneNumberPairsInNDays():DataFrame =
  {
    var regionLimitationSql = ""
    if (idsOfSelectedRegions != null && idsOfSelectedRegions.nonEmpty) {
      val idOfFirstRegion = idsOfSelectedRegions(0)
      var sqlTemp = s"region_ids LIKE '%$idOfFirstRegion%' "
      for (i <- 1 until idsOfSelectedRegions.length - 1) {
        val idOfRegion = idsOfSelectedRegions(i)
        sqlTemp = sqlTemp + s"""OR region_ids LIKE '%$idOfRegion%' """
      }
      regionLimitationSql = s"AND ($sqlTemp)"
    }

    var dataLimitSql = ""
    if (!dataLimitSql.isEmpty) {
      dataLimitSql = s"LIMIT $data_size_limit"
    }

    pnp = spark.sql(
      s"""
         |SELECT
         |    mobile_number, phone_number
         |FROM
         |    (SELECT phone_number, user_id FROM dwd.dwd_tantan_eventlog_user_contact_i_d
         |    WHERE dt between '$i_user_contact_start_date' and '$i_user_contact_end_date'
         |    $dataLimitSql)
         |JOIN
         |    (SELECT mobile_number, id FROM dwd.dwd_putong_yay_users_a_d
         |    WHERE dt = '$a_user_table_date' $regionLimitationSql)
         |on
         |    user_id = id
    """.stripMargin
    ).rdd.map{
      case Row(null, _) => null
      case Row(_, null) => null
      case Row(srcNode: String, destNode: String) => (srcNode, destNode)
    }.filter(_!=null).toDF("mobile_number", "phone_number")
    pnp
  }

  def getDistinctPhoneNum(): Unit = {
    val contactsPhoneNums = pnp.select("phone_number")
    val usersPhoneNums = pnp.select("mobile_number")
    distinctPhoneNumbers = contactsPhoneNums.union(usersPhoneNums).distinct()
  }

  def getOutDegree(): DataFrame = {
    outDegree = pnp.rdd
      .map{case Row(user_number: String, _) => (user_number, 1.toLong)}
      .reduceByKey(_ + _)
      .map{case (user_number: String, num) => (num, user_number)}
      .sortByKey().toDF("out_degree", "phone_num")
    outDegree
  }

  def getInDegree(): DataFrame = {
    inDegree = pnp.rdd
      .map{case Row(_, contact_number: String) => (contact_number, 1.toLong)}
      .reduceByKey(_ + _)
      .map{case (contact_number: String, num) => (num, contact_number)}
      .sortByKey().toDF("in_degree", "phone_num")
    inDegree
  }
}
