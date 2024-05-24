package live.ashish.spark.etl.usecases

import live.ashish.spark.etl.InitSpark
  import net.schmizz.sshj.SSHClient
  import net.schmizz.sshj.transport.verification.PromiscuousVerifier

object SFTPConnectorSpark extends InitSpark {


def downloadFileFromSFTP(username: String, password: String, host: String, remoteFilePath: String, localFilePath: String): Unit = {
  val sshClient = new SSHClient()
  sshClient.addHostKeyVerifier(new PromiscuousVerifier())
  try {
    sshClient.connect(host)
    sshClient.authPassword(username, password)
    val sftpClient = sshClient.newSFTPClient()
    try {
      sftpClient.get(remoteFilePath, localFilePath)
    } finally {
      sftpClient.close()
    }
  } finally {
    sshClient.disconnect()
  }
}
  def main(args: Array[String]) = {
    val localFilePath="/Users/ashishpatel/prophecy-dev/spark-kafka-gradle/test.txt"
    downloadFileFromSFTP(username = "sftp_user", password = "Prophecy@123", host= "ec2-13-52-123-33.us-west-1.compute.amazonaws.com",
      remoteFilePath = "/sftp_user/ashish/test.txt", localFilePath = localFilePath)

     spark.read.format("text").load(localFilePath).show()
    close
  }


}
