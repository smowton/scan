import org.broadinstitute.sting.queue.QScript
import org.broadinstitute.sting.queue.function.CommandLineFunction
import org.broadinstitute.sting.commandline.{Input, Output}

import java.io.File

class HelloWorldProc extends CommandLineFunction {

  def commandLine = "echo \"Hello World!\"; sleep 30"
  jobQueue = "linux"
  jobNativeArgs = List("mempercore", "2", "estsize", "100")
  nCoresRequest = Some(4)
  
  @Input val nonscanfs = new File("/tmp/hello")
  @Input val scanfs = new File("scanfs:/world")
  @Output val nonscanout = new File("/tmp/goodbye")
  @Output val scanout = new File("scanfs:/everyone")

}

class HelloWorldScript extends QScript {

  def script {

    add(new HelloWorldProc)

  }

}
