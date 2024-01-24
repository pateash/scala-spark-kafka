package live.ashish.spark.etl.utils.formats

import live.ashish.spark.etl.InitSpark

object SparkGenerateFile extends InitSpark{

  def main(args: Array[String]): Unit = {

  }


  def concatenateFiles(
    spark:                SparkSession,
    format:               String,
    mode:                 String,
    inputDir:             String,
    outputFileName:       String,
    deleteTempPath:       Boolean = true,
    fileFormatHasHeaders: Boolean = false
  ): Unit = {
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
    import org.apache.hadoop.io.IOUtils

    import java.io._
    import scala.util.Try

    val inputDirPath   = new Path(inputDir)
    val mergedFilePath = new Path(outputFileName)

    // Create a Hadoop configuration object
    val hadoopConfig = new Configuration()
    val srcFs        = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outputFile   = srcFs.create(mergedFilePath)

    // Get the list of files in the input directory
    val files = srcFs.listStatus(inputDirPath).map(_.getPath)

    // Read in the contents of all input CSV files, skipping the header of all but the first file
    var counter = 1

    if (mode == "error" && srcFs.exists(mergedFilePath)) {
      srcFs.delete(inputDirPath, true)
      throw new IOException(
        s"Target already exists. Please delete the target first to write it with error mode, or change the write mode to overwrite."
      )
    } else if (srcFs.exists(mergedFilePath) && mode == "ignore") {
      // Do nothing
    } else {
      if (srcFs.exists(mergedFilePath) && mode == "append") {
        val existingFile = srcFs.open(mergedFilePath)
        Try(IOUtils.copyBytes(existingFile, outputFile, hadoopConfig, false))
        if (fileFormatHasHeaders) {
          counter = counter + 1
        }
        existingFile.close()
      }

      files.foreach { file ⇒
        if (file.getName.endsWith(format)) {
          val inputFile = srcFs.open(file)
          if (counter == 1) {
            // Use the header from the first file
            Try(IOUtils.copyBytes(inputFile, outputFile, hadoopConfig, false))
          } else {
            // Skip the header from all other files
            inputFile.readLine()
            val newInputStream: FSDataInputStream = srcFs.open(file)
            newInputStream.seek(inputFile.getPos())
            Try(IOUtils.copyBytes(newInputStream, outputFile, hadoopConfig, false))
            newInputStream.close()
          }
          if (fileFormatHasHeaders) {
            counter = counter + 1
          }
          inputFile.close()
        }
      }
      outputFile.close()
    }
    if (deleteTempPath) {
      srcFs.delete(inputDirPath, true)
    }
  }
}
