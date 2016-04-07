import org.apache.spark._

object mainTest extends App {
 override def main (args: Array[String]): Unit = {
    println("Hello, world!");
    
    val conf = new SparkConf()
         .setAppName("testApp");
    
    
    val sc = new SparkContext(conf);
    
    val file = sc.textFile("C:\\Users\\TeamVictor\\Desktop\\a.txt");
  }
 }
