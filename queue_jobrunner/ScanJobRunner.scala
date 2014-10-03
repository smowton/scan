import org.broadinstitute.sting.queue.engine.shell.{ShellJobManager, ShellJobRunner}
import org.broadinstitute.sting.queue.engine.RunnerStatus
import org.broadinstitute.sting.queue.function.CommandLineFunction

import java.net.{URL, URLEncoder}

import org.json.{JSONTokener, JSONObject}

class ScanJobRunner(function: CommandLineFunction, val manager: ScanJobManager) extends ShellJobRunner(function) {

  val className : String = "linux"
  var jobId : String = _

  override def start() {

    val escaped_cmd = URLEncoder.encode(jobScript.toString, "UTF-8")
    val url = "http://%s:%d/addworkitem?classname=%s&fsreservation=0&dbreservation=0&cmd=%s".format(
      manager.scanHost, manager.scanPort, className, escaped_cmd)
    val stream = new URL(url).openStream()
    val tok = new JSONTokener(stream)
    val reply_obj = new JSONObject(tok)

    jobId = reply_obj.getString("pid")

    updateStatus(RunnerStatus.RUNNING)
    logger.info("Submitted job id: " + jobId)

  }

  def updateJobStatus(procmap : Map[(String, String), JSONObject]) = {

    procmap.get((className, jobId)) match {

      case None => {
        updateStatus(RunnerStatus.DONE)
        logger.info("Job id " + jobId + " done")
      }
      case Some(jsobj) => updateStatus(RunnerStatus.RUNNING)

    }

  }

  override def tryStop() { }

}

