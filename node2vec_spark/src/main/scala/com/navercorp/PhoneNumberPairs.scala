package com.navercorp

import org.apache.spark.sql.SparkSession

class PhoneNumberPairs(
                        val spark: SparkSession,
                        var i_user_contact_start_date: String = "",
                        var i_user_contact_end_date: String = "",
                        var a_user_table_date: String = "",
                        var idsOfSelectedRegions: Array[String] = null
                      ) extends Serializable {

  import org.apache.spark.sql.{DataFrame, SparkSession, Row}
  import spark.implicits._

  var pnp: DataFrame = _
  var distinctPhoneNumbers: DataFrame = _
  var phoneCount: Long = _
  var edgeCount: Long = _
  var outDegreeForEachPhoneNum: DataFrame = _
  var inDegreeForEachPhoneNum: DataFrame = _
  var outDegreeVersusTotalNumber: DataFrame = _
  var inDegreeVersusTotalNumber: DataFrame = _

  def regexCheckAndSlicePhoneNumber(phoneNumber: String) = {
    if (phoneNumber.matches("^(|0*86)(1[34578]\\d{9})$"))
      phoneNumber.takeRight(11)
    else ""
  }

  def getPhoneNumberPairsEfficiently() = {

  }


  def getPhoneNumberPairs(): DataFrame = {
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


    pnp = spark.sql(
      s"""
         |SELECT
         |    mobile_number, phone_number
         |FROM
         |    (SELECT phone_number, user_id FROM dwd.dwd_tantan_eventlog_user_contact_i_d
         |    WHERE dt between '$i_user_contact_start_date' and '$i_user_contact_end_date')
         |JOIN
         |    (SELECT mobile_number, id FROM dwd.dwd_putong_yay_users_a_d
         |    WHERE dt = '$a_user_table_date' $regionLimitationSql)
         |on
         |    user_id = id
    """.stripMargin
    ).rdd.map {
      case Row(null, _) => null
      case Row(_, null) => null
      case Row(srcNode: String, destNode: String) => {
        val srcNodeTemp = regexCheckAndSlicePhoneNumber(srcNode)
        val destNodeTemp = regexCheckAndSlicePhoneNumber(destNode)
        if (srcNodeTemp.isEmpty || destNodeTemp.isEmpty || srcNodeTemp == destNodeTemp) null
        else (srcNodeTemp, destNodeTemp)
      }
    }.filter(_ != null)
      .distinct()
      .toDF("mobile_number", "phone_number")
    pnp
  }

  def getDistinctPhoneNum(): Unit = {
    val contactsPhoneNums = pnp.select("phone_number")
    val usersPhoneNums = pnp.select("mobile_number")
    distinctPhoneNumbers = contactsPhoneNums.union(usersPhoneNums).distinct()
  }

  def getOutDegreeForEachPhoneNum(): DataFrame = {
    outDegreeForEachPhoneNum = pnp.rdd
      .map { case Row(user_number: String, _) => (user_number, 1.toLong) }
      .reduceByKey(_ + _)
      .map { case (user_number: String, num) => (num, user_number) }
      .sortByKey().toDF("out_degree", "phone_num")
    outDegreeForEachPhoneNum
  }

  def getInDegreeForEachPhoneNum(): DataFrame = {
    inDegreeForEachPhoneNum = pnp.rdd
      .map { case Row(_, contact_number: String) => (contact_number, 1.toLong) }
      .reduceByKey(_ + _)
      .toDF("in_degree", "phone_num")
    inDegreeForEachPhoneNum
  }

  def joinInDegreeAndOutDegree(): DataFrame = {
    inDegreeForEachPhoneNum.join(outDegreeForEachPhoneNum)
  }

  def getOutDegreeVerseTotalNumber(minOutDegree: Int, maxOutDegree: Int): DataFrame = {
    outDegreeVersusTotalNumber = outDegreeForEachPhoneNum.rdd
      .map { case Row(_, out_degree: Long) => (out_degree, 1.toLong) }
      .reduceByKey(_ + _)
      .filter { case (out_degree, _) => out_degree > minOutDegree && out_degree < maxOutDegree }
      .toDF("out_degree", "total_num")
    outDegreeVersusTotalNumber
  }

  def getInDegreeVerseTotalNumber(minInDegree: Int, maxInDegree: Int): DataFrame = {
    inDegreeVersusTotalNumber = inDegreeForEachPhoneNum.rdd
      .map { case Row(in_degree: Long, _) => (in_degree, 1.toLong) }
      .reduceByKey(_ + _)
      .filter { case (in_degree, _) => in_degree > minInDegree && in_degree < maxInDegree }
      .toDF("in_degree", "total_num")
    inDegreeVersusTotalNumber
  }

  def saveInDegreeAndOutDegreeAsCsv(path: String): Unit = {
    inDegreeVersusTotalNumber.repartition(1).write.format("csv").save(path.concat("/inDegree"))
    outDegreeVersusTotalNumber.repartition(1).write.format("csv").save(path.concat("/outDegree"))
  }
}
