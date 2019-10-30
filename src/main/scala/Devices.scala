import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Devices {

def cleanData(data: String): String = {
  val splitData = data.split('=')
  splitData(1).replaceAll("[{}\" ]", "")    // removing all dirty characters resulted from reading raw text file
}

def main(args: Array[String]) {
    if (args.length != 1) {
    Console.err.println("Usage: scala-jar <path to input file> ")
    System.exit(1)
    }
    val inputFile = args(0) //"/home/bethu/Downloads/scala/input.txt" // Should be some file on your system
    val sc = new SparkContext(new SparkConf())
    val rawData = sc.textFile(inputFile)
    val xdata = rawData.map(x => cleanData(x))
    val scoreData = xdata.map(x => ((x.split(',')(0),x.split(',')(1)), x.split(',')(2).toFloat))
                    .mapValues(value => (value, 1))
                    .reduceByKey{
                      case ((sumL, countL), (sumR, countR)) => (sumL+sumR, countL+countR)
                    }
                    .mapValues {
                      case (sum, count) => sum/count
                    }

    val poorTuple = scoreData.map{
      case ((id: String, mobile: String), score: Float) => {(mobile, {if(score < 50) 1.0f else 0.0f})}
    }
    .mapValues(value => (value, 1))
    .reduceByKey{
      case ((sumL, countL), (sumR, countR)) => (sumL+sumR, countL+countR)
    }
    .mapValues {
      case (sum, count) => sum/count
    }
    .reduce((acc,value) => {
      if(acc._2 < value._2) value else acc})

    println(poorTuple._1)
    sc.stop()
  }
}
