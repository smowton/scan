import org.broadinstitute.sting.queue.engine.{CommandLineJobRunner, RunnerStatus}
import org.broadinstitute.sting.queue.function.CommandLineFunction

import java.net.{URL, URLEncoder}

import org.json.{JSONTokener, JSONObject}

class ScanJobRunner(val function: CommandLineFunction, val manager: ScanJobManager) extends CommandLineJobRunner {

  val className : String = function.jobQueue
  var jobId : Long = _

  override def start() {

    val cmd_with_redir = "sh " + jobScript.toString + " > " + function.jobOutputFile.getPath
    val cmd_with_err = cmd_with_redir + " " + (if (function.jobErrorFile == null) "2>&1" else function.jobErrorFile.getPath)

    val escaped_cmd = URLEncoder.encode(cmd_with_err, "UTF-8")
    val url = "http://%s:%d/addworkitem?classname=%s&fsreservation=0&dbreservation=0&cmd=%s".format(
      manager.scanHost, manager.scanPort, className, escaped_cmd)
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

  def tryStop() { }

}

