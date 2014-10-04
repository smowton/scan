import org.broadinstitute.sting.queue.engine.CommandLineJobManager
import org.broadinstitute.sting.queue.function.CommandLineFunction

import java.net.URL

import scala.collection.JavaConversions._

import org.json.{JSONTokener, JSONObject}

class ScanJobManager extends CommandLineJobManager[ScanJobRunner] {

  def runnerType = classOf[ScanJobRunner]
  def create(function: CommandLineFunction) = new ScanJobRunner(function, this)

  val env = System.getenv()
  val scanHost = (if(env.containsKey("SCAN_HOST")) env.get("SCAN_HOST"); else "localhost")
  val scanPort = (if(env.containsKey("SCAN_PORT")) Integer.parseInt(env.get("SCAN_PORT")); else 8080)

  override def updateStatus(runners : Set[ScanJobRunner]) : Set[ScanJobRunner] = {

    try {

    val url = "http://%s:%d/lsallprocs".format(scanHost, scanPort)
    val stream = new URL(url).openStream()
    val tok = new JSONTokener(stream)
    val classes = new JSONObject(tok)

    var mappings = List[((String, Long), JSONObject)]()

    for(classname <- classes.keys) {

      val ids = classes.getJSONObject(classname)
      for(id <- ids.keys) {
        
        val id_long = id.toLong
        mappings :+= ((classname, id_long), ids.getJSONObject(id))

      }

    }

    val procmap : Map[(String, Long), JSONObject] = Map(mappings:_*)

    for(runner <- runners) {
      runner.updateJobStatus(procmap)
    }

    return runners

    }
    catch {
      case x : Exception => { x.printStackTrace(System.err); runners }

    }

  }

  override def tryStop(runners : Set[ScanJobRunner]) {

    runners.foreach(_.tryStop())

  }

}
