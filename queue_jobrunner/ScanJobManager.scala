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

    val url = "http://%s:%d/lsprocs".format(scanHost, scanPort)
    val stream = new URL(url).openStream()
    val tok = new JSONTokener(stream)
    val ids = new JSONObject(tok)

    var mappings = List[(Long, JSONObject)]()

    for(id <- ids.keys) {
        
      val id_long = id.toLong
      mappings :+= (id_long, ids.getJSONObject(id))

    }

    val procmap : Map[Long, JSONObject] = Map(mappings:_*)

    val url2 = "http://%s:%d/lscompletedprocs".format(scanHost, scanPort)
    val stream2 = new URL(url2).openStream()
    val tok2 = new JSONTokener(stream2)
    val ids2 = new JSONObject(tok2)

    var rclist = List[(Long, Long)]()

    for(id <- ids2.keys) {

      val id_long = id.toLong
      val rc = ids.getLong(id)
      rclist :+= (id_long, rc)

    }

    val rcmap : Map[Long, Long] = Map(rclist:_*)

    for(runner <- runners) {
      runner.updateJobStatus(procmap, rcmap)
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
