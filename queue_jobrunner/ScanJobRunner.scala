import org.broadinstitute.sting.queue.engine.{CommandLineJobRunner, RunnerStatus}
import org.broadinstitute.sting.queue.function.CommandLineFunction

import java.net.{URL, URLEncoder}

import org.json.{JSONTokener, JSONObject}

class ScanJobRunner(val function: CommandLineFunction, val manager: ScanJobManager) extends CommandLineJobRunner {

  val className : String = function.jobQueue
  var jobId : Long = _

  def getIntegerNativeParam(name : String, defaultValue : Integer) : Integer = {

    jobScript.jobNativeArgs.find(_ == name) match {
      None => defaultValue |
      Some(x) => Integer.parseInt(jobScript.jobNativeArgs(x + 1))
    }

  }

  def getScanfsFiles(in : Seq[File]) : String = {

    val scanfsFiles = in.filter(file => file.isAbsolute() && file.getPath().startsWith("scanfs:/"))
    val absPaths = scanfsFiles.map(file => file.toString().replace("scanfs:/", "/mnt/scanfs/"))
    absPaths.mkString(",")  

  } 

  override def start() {

    val cmd_with_redir = "sh " + jobScript.toString + " > " + function.jobOutputFile.getPath
    val cmd_with_err = cmd_with_redir + " " + (if (function.jobErrorFile == null) "2>&1" else function.jobErrorFile.getPath)

    val escaped_cmd = URLEncoder.encode(cmd_with_err, "UTF-8")
    
    // Fish out args that aren't well expressed by existing Function members:
    val memPerCore = getIntegerNativeParam("mempercore")
    val estSize = getIntegerNativeParam("estsize")

    val declareInputs = getScanfsFiles(jobScript.inputs)
    val declareOutputs = getScanfsFiles(jobScript.outputs)

    val url = "http://%s:%d/addworkitem?classname=%s&maxcores=%d&mempercore=%d&estsize=%d&filesin=%s&filesout=%s&cmd=%s".format(
      manager.scanHost, manager.scanPort, className, jobScript.nCoresRequest.getOrElse(1), memPerCore, estSize, declareInputs, declareOutputs, escaped_cmd)
    val stream = new URL(url).openStream()
    val tok = new JSONTokener(stream)
    val reply_obj = new JSONObject(tok)

    jobId = reply_obj.getLong("pid")

    updateStatus(RunnerStatus.RUNNING)
    logger.info("Submitted job id: " + jobId)

  }

  def updateJobStatus(procmap : Map[(String, Long), JSONObject]) = {

    logger.info("Check job status %s %d".format(className, jobId))

    procmap.get((className, jobId)) match {

      case None => {
        updateStatus(RunnerStatus.DONE)
        logger.info("Job id " + jobId + " done")
      }
      case Some(jsobj) => updateStatus(RunnerStatus.RUNNING)

    }

  }

  def tryStop() { 

    val url = "http://%s:%d/delworkitem?tid=%d".format(jobScript.jobId)
    val stream = new URL(url).openStream()
    val tok = new JSONTokener(stream)
    val reply_obj = new JSONObject(tok)
    logger.info("Killing process %d: %s".format(jobId, reply_obj.getString("status")))

  }

}

