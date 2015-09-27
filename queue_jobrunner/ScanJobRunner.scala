import org.broadinstitute.sting.queue.engine.{CommandLineJobRunner, RunnerStatus}
import org.broadinstitute.sting.queue.function.CommandLineFunction

import java.net.{URL, URLEncoder}
import java.io.File

import org.json.{JSONTokener, JSONObject}

class ScanJobRunner(val function: CommandLineFunction, val manager: ScanJobManager) extends CommandLineJobRunner {

  val className : String = function.jobQueue
  var jobId : Long = _

  def getIntegerNativeParam(name : String, defaultValue : Integer) : Integer = {

    function.jobNativeArgs.indexOf(name) match {
      case -1 => defaultValue
      case x => Integer.parseInt(function.jobNativeArgs(x + 1))
    }

  }

  val scanfsPrefix = "/mnt/scanfs/"

  def getScanfsFiles(in : Seq[File]) : String = {

    val scanfsFiles = in.filter(file => file.getPath().startsWith(scanfsPrefix))
    val absPaths = scanfsFiles.map(file => file.toString().substring(scanfsPrefix.length()))
    absPaths.mkString(":")  

  } 

  override def start() {

    val cmd_with_redir = "sh " + jobScript.toString + " > " + function.jobOutputFile.getPath
    val cmd_with_err = cmd_with_redir + " " + (if (function.jobErrorFile == null) "2>&1" else function.jobErrorFile.getPath)

    val escaped_cmd = URLEncoder.encode(cmd_with_err, "UTF-8")
    
    // Fish out args that aren't well expressed by existing Function members:
    val memPerCore = getIntegerNativeParam("mempercore", 1)
    val estSize = getIntegerNativeParam("estsize", 1)
    val maxCores = function.nCoresRequest.getOrElse(getIntegerNativeParam("maxcores", 1))

    val declareInputs = getScanfsFiles(function.inputs)
    val declareOutputs = getScanfsFiles(function.outputs)

    val url = "http://%s:%d/addworkitem?classname=%s&maxcores=%d&mempercore=%d&estsize=%d&filesin=%s&filesout=%s&cmd=%s".format(
      manager.scanHost, manager.scanPort, className, maxCores, memPerCore, estSize, declareInputs, declareOutputs, escaped_cmd)
    val stream = new URL(url).openStream()
    val tok = new JSONTokener(stream)
    val reply_obj = new JSONObject(tok)

    jobId = reply_obj.getLong("pid")

    updateStatus(RunnerStatus.RUNNING)
    logger.info("Submitted job id: " + jobId)

  }

  def updateJobStatus(procmap : Map[Long, JSONObject], rcmap : Map[Long, Long]) = {

    logger.info("Check job status %d".format(jobId))

    procmap.get(jobId) match {

      case None => {

	rcmap.get(jobId) match {
	  
	  case Some(0) => {

	    updateStatus(RunnerStatus.DONE)
	    logger.info("Job id " + jobId + " done")

	  }
	  case _ => {

	    updateStatus(RunnerStatus.FAILED)
	    logger.info("Job id " + jobId + " failed (returned " + rcmap.get(jobId) + ")")

	  }

	}

      }
      case Some(jsobj) => updateStatus(RunnerStatus.RUNNING)

    }

  }

  def tryStop() { 

    val url = "http://%s:%d/delworkitem?tid=%d".format(jobId)
    val stream = new URL(url).openStream()
    val tok = new JSONTokener(stream)
    val reply_obj = new JSONObject(tok)
    logger.info("Killing process %d: %s".format(jobId, reply_obj.getString("status")))

  }

}

