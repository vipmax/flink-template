import org.apache.flink.streaming.api.scala._

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text sever (at port 12345)
 * using the ''netcat'' tool via
 * {{{
 * nc -l 12345 on Linux or nc -l -p 12345 on Windows
 * }}}
 * and run this example with the hostname and the port as arguments..
 */
object SocketWindowWordCount {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // RUN nc -l 1234 BEFORE
    var hostname = "localhost"
    var port = 1234


    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream(hostname, port, '\n')

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
//      .timeWindow(Time.seconds(5))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)
}

// MORE EXAMPLES HERE
// https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples