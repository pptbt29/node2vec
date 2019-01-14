package com.navercorp

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataGenerator {
  def getPhoneNumberPairsInNDays(spark: SparkSession,
                                 i_user_contact_start_date: String = "",
                                 i_user_contact_end_date: String = "",
                                 a_user_table_date: String = "",
                                 data_size_limit: String = "",
                                 idsOfSelectedRegions: Array[String] = null
                                ):DataFrame =
  {
    var regionLimitationSql = ""
    if (idsOfSelectedRegions != null && idsOfSelectedRegions.length != 0) {
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

    spark.sql(
      s"""
         |SELECT
         |    phone_number, mobile_number
         |FROM
         |    (SELECT phone_number, user_id FROM dwd.dwd_tantan_eventlog_user_contact_i_d
         |    WHERE dt between '$i_user_contact_start_date' and '$i_user_contact_end_date')
         |JOIN
         |    (SELECT mobile_number, id FROM dwd.dwd_putong_yay_users_a_d
         |    WHERE dt = '$a_user_table_date')
         |on
         |    user_id = id
    """.stripMargin
    ).rdd.map {
      case Row(null, _) => null
      case Row(_, null) => null
      case Row(srcNode: String, destNode: String) => {
        val srcNodeTemp = regexCheckAndSlicePhoneNumber(srcNode)
        val destNodeTemp = regexCheckAndSlicePhoneNumber(destNode)
        (srcNodeTemp, destNodeTemp, srcNode, destNode)
      }
    }.filter(_ != null).toDF("mobile_number", "phone_number", "mobile_number_origin", "phone_number_origin")

  }
}
