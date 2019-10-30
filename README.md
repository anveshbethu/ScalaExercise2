# ScalaExercise2
Finding the poor performance

Execution:
> sbt compile
> sbt package
jar file generates in target/scala dir
> sparkHome/bin/spark-submit --class "Devices" --master *host[*number of threads] /*path to jar file from sbt package*/devices_2.11-1.0.jar /*path to*/input.txt

example on a local:
> spark/bin/spark-submit --class "Devices" --master local[1] /home/bethu/scala/sparkScala/target/scala-2.11/devices_2.11-1.0.jar /home/bethu/Downloads/scala/input.txt
