package org.itmo

import kotlin.system.exitProcess
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
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
            logger.info("Reduction job failed!")
            exitProcess(1)
        }

        val sortingJob = setupSortingJob(intermediateOutput, Path(outputPath))
        if (!sortingJob.waitForCompletion(false)) {
            logger.info("Sorting job failed!")
            exitProcess(1)
        }
    }

    private fun setupReductionJob(inputPath: Path, outputPath: Path): Job {
        val configuration = Configuration().apply {
            set(TextOutputFormat.SEPARATOR, ",")
        }

        val fs = FileSystem.get(configuration)

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true)
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

        val fs = FileSystem.get(configuration)

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true)
        }

        return Job.getInstance(configuration).apply {
            setJarByClass(Main::class.java)
            mapperClass = Sorter.RecordHandler::class.java
            mapOutputKeyClass = DoubleWritable::class.java
            mapOutputValueClass = SalesRecord::class.java
            reducerClass = Sorter.RevenueReducer::class.java
            outputKeyClass = Text::class.java
            outputValueClass = Text::class.java

            FileInputFormat.addInputPath(this, inputPath)
            FileOutputFormat.setOutputPath(this, outputPath)

            outputPath.getFileSystem(configuration).delete(outputPath, true)
        }
    }
}
