import org.broadinstitute.sting.queue.function.CommandLineFunction

class MyCmd extends CommandLineFunction {

  def commandLine : String = "sleep 5"

}

object Test {

  def main(args:Array[String]) {

    val c = new MyCmd
    val manager = new ScanJobManager

    val runner = manager.create(c)
    manager.updateStatus(Set(runner))

  }

}
