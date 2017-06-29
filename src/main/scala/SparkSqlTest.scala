import org.apache.spark._
import org.apache.spark.SparkContext._

object SparkSqlTest{
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("wordCount")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val df = sqlContext.read.parquet("/user/hive/warehouse/galaxies2/000000_0")
      df.show()
    }
}
