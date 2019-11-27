
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.fromElements(
      "To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer",
      "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,"
    )

    val words = text.flatMap { line => line.toLowerCase.split("\\W+") }

    val wordsPairs = words.map { w => (w, 1) }

    val groupedCountPairs = wordsPairs.groupBy(0)

    val wordCounts = groupedCountPairs.sum(1)

    wordCounts.print()
  }
}