package com.seungwoo.test

import com.google.gson.Gson


object TestOthers {
  def main(args: Array[String]): Unit = {
    val gson = new Gson()
    gson.toJson(new UserBehiver("1267168", "wuseungwoo", 1259354543L))

    val data:String =
      """
        |{"id":"1267168","name":"wuseungwoo","time":1259354543}
      """.stripMargin
    val behiver: UserBehiver = gson.fromJson(data,classOf[UserBehiver])

    println(behiver.id)

  }
}
case class UserBehiver(id: String, name: String, time: Long)
