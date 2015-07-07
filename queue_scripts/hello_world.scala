import org.broadinstitute.sting.queue.QScript
import org.broadinstitute.sting.queue.function.CommandLineFunction

class HelloWorldProc extends CommandLineFunction {

  def commandLine = "echo \"Hello World!\""
  jobQueue = "linux"

}

class HelloWorldScript extends QScript {

  def script {

    add(new HelloWorldProc)

  }

}
