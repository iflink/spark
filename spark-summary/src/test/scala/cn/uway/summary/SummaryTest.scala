package scala.cn.uway.summary

import cn.uway.counter.mdtweek.Nei

object SummaryTest {
  def main(args: Array[String]): Unit = {
   val neiSeq = Seq(
     ("a", Nei(1, 1, 1, 10.0f, 1)),
     ("b", Nei(2, 2, 2, 70.0f, 1)),
     ("c", Nei(3, 3, 3, 14.0f, 1)),
     ("d", Nei(4, 4, 4, 1.0f, 1)),
     ("e", Nei(5, 5, 5, 30.0f, 1))
   )
    val neiSortSeq = neiSeq.sortWith((o1, o2) => {
      o1._2.getAvgRsrp() > o2._2.getAvgRsrp()
    })

    var index = 0
    for(o <- neiSortSeq if index < 4) {
      index += 1
      print(o)
    }
  }

  def printr(args: Int*): Unit = {
    println(args(0), args(1))
  }
}