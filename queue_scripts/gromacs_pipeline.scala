import org.broadinstitute.sting.queue.QScript
import org.broadinstitute.sting.queue.function.CommandLineFunction
import org.broadinstitute.sting.commandline.{Input, Output}

import java.io.File

object PH {

  def pathjoin(root : String, branch : String) : String = {
    
    val rootslash = root.endsWith("/");
    val branchslash = branch.startsWith("/");

    if(rootslash && branchslash)
      root.substring(0, root.length() - 1) + branch;
    else if(!(rootslash || branchslash))
      root + "/" + branch;
    else
      root + branch;

  }

}

class GenericCmd(ins : Seq[File], outs : Seq[File], cmd : String) extends CommandLineFunction {

  @Input
    var ins2 = ins;

  @Output
    var outs2 = outs;

  def commandLine : String = cmd

}

class GromacsScript extends QScript {

  @Argument
  var workdir : String = _

  @Argument
  var estsize : String = _

  def gromadd(c : CommandLineFunction, cores : Integer, classname : String) {

    c.jobNativeArgs = List("estsize", estsize, "mempercore", "1")
    c.commandDirectory = workdir
    c.jobLocalDir = "/tmp"
    c.jobQueue = classname
    c.nCoresRequest = Some(cores)
    
    add(c)

  }

  def script {

    val stages = List("input", "EM", "NVT", "NPT", "main")
    
    def makegrom(from : String, to : String) {

      val topin = PH.pathjoin(workdir, from + ".top")
      val groin = PH.pathjoin(workdir, from + ".gro")
      val posrein = PH.pathjoin(workdir, "posre.itp")
      val mdpin = PH.pathjoin(workdir, to + ".mdp")
      val topout = PH.pathjoin(workdir, to + ".top")
      val tprout = PH.pathjoin(workdir, to + ".tpr")

      val cmd = List("/usr/local/gromacs/bin/grompp", "-f", mdpin, "-c", groin, "-p", topin, "-pp", topout, "-o", tprout).mkString(" ")
      gromadd(new GenericCmd(List(new File(topin), new File(groin), new File(posrein), new File(mdpin)), List(new File(topout), new File(tprout)), cmd), 1, "gmx_grompp")

    }

    (0 to 3).map(i => makegrom(stages(i), stages(i + 1)))

    def makemd(stage : String) {

      val tprin = PH.pathjoin(workdir, stage + ".tpr");
      val topin = PH.pathjoin(workdir, stage + ".top");
      val trrout = PH.pathjoin(workdir, stage + ".trr");
      val groout = PH.pathjoin(workdir, stage + ".gro");

      val cmd = List("/usr/local/gromacs/bin/mdrun", "-v", "-deffnm", stage, "-ntomp", "$SCAN_CORES").mkString(" ");
      gromadd(new GenericCmd(List(new File(tprin), new File(topin)), List(new File(trrout), new File(groout)), cmd), 16, "gmx_" + stage.toLowerCase())

    }

    stages.drop(1).map(makemd)

  }

}
