package sample.gen

import scala.util.Random

object GenerateOrdersCSV {
  def main(args: Array[String]): Unit = {
    val random = new Random()
    val startHour = 0
    val startMinute = 0
    val startSecond = 0
    val data = (1 to 10000).map { i =>
      val id = i
      val customer_id = random.nextInt(100) + 1
      val hour = startHour + (i / 3600)
      val minute = startMinute + ((i % 3600) / 60)
      val second = startSecond + (i % 60)
      val order_date = f"2022-01-01 $hour%02d:$minute%02d:$second%02d"

      val status = if (random.nextBoolean()) "pending" else "paid"
      val total_amount = BigDecimal(random.nextDouble() * 1000).setScale(2, BigDecimal.RoundingMode.HALF_UP)

      s"$id,$customer_id,$order_date,$status,$total_amount"
    }

    val writer = new java.io.FileWriter("data/OrdersAnalysis.csv")
    writer.write("id,customer_id,order_date,status,total_amount\n")
    data.foreach { line =>
      writer.write(line + "\n")
    }
    writer.close()
  }
}