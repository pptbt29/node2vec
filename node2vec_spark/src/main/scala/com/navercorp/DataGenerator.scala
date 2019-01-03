package com.navercorp

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataGenerator {

  def getPhoneNumberPairsInNDays(spark: SparkSession,
                                 i_user_contact_start_date: String,
                                 i_user_contact_end_date: String, a_user_table_date:
                                 String, data_size_limit: String
                                ):DataFrame =
  {
    spark.sql(
      s"""
         |SELECT
         |    phone_number, mobile_number
         |FROM
         |    (SELECT phone_number, user_id FROM dwd.dwd_tantan_eventlog_user_contact_i_d
         |    WHERE dt between '$i_user_contact_start_date' and '$i_user_contact_end_date'
         |    LIMIT $data_size_limit)
         |JOIN
         |    (SELECT mobile_number, id FROM dwd.dwd_putong_yay_users_a_d WHERE dt = '$a_user_table_date')
         |on
         |    user_id = id
    """.stripMargin
    )
  }
}
