package org.itmo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.log4j.Logger

private val logger = Logger.getLogger(Main::class.java)

object Main {

    const val inputPath = "data"
    const val outputPath = "output"

    @JvmStatic
    fun main(args: Array<String>) {
        val intermediateOutput = Path("tmp")
        val reductionJob = setupReductionJob(Path(inputPath), intermediateOutput)
        if (!reductionJob.waitForCompletion(false)) {
            logger.info { "Reduction job failed!" }
            return
        }

        val sortingJob = setupSortingJob(intermediateOutput, Path(outputPath))
        if (!sortingJob.waitForCompletion(false)) {
            logger.info { "Sorting job failed!" }
            return
        }
    }

    private fun setupReductionJob(inputPath: Path, outputPath: Path): Job {
        val configuration = Configuration().apply {
            set(TextOutputFormat.SEPARATOR, ",")
        }

        return Job.getInstance(configuration).apply {
            setJarByClass(Main::class.java)
            mapperClass = LineProcessor::class.java

            mapOutputKeyClass = Text::class.java
            mapOutputValueClass = DataRecord::class.java

            reducerClass = DataAggregator::class.java

            outputKeyClass = Text::class.java
            outputValueClass = DataRecord::class.java

            FileInputFormat.addInputPath(this, inputPath)
            FileOutputFormat.setOutputPath(this, outputPath)

            outputPath.getFileSystem(configuration).delete(outputPath, true)
        }
    }

    private fun setupSortingJob(inputPath: Path, outputPath: Path): Job {
        val configuration = Configuration()

        return Job.getInstance(configuration).apply {
            setJarByClass(Main::class.java)
            mapperClass = Sorter.RecordHandler::class.java

            mapOutputKeyClass = SalesRecord::class.java
            mapOutputValueClass = Text::class.java

            reducerClass = Sorter.RevenueReducer::class.java

            outputKeyClass = SalesRecord::class.java
            outputValueClass = Text::class.java

            FileInputFormat.addInputPath(this, inputPath)
            FileOutputFormat.setOutputPath(this, outputPath)

            outputPath.getFileSystem(configuration).delete(outputPath, true)
        }
    }
}
