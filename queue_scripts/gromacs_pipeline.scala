
object PH {

  def pathjoin(root, branch) : String = {
    
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
    var inputs = ins;

  @Output
    var outputs = outs;

  def commandLine : String = cmd

}

class GromacsScript extends QScript {

  @Argument
  var workdir : String = _

  def script {

    val stages = List("EM", "NVT", "NPT", "main")
    
    def makegrom(from : String, to : String) {

      val topin = PH.pathjoin(workdir, from + ".top")
      val groin = PH.pathjoin(workdir, from + ".gro")
      val posrein = PH.pathjoin(workdir, "posre.itp")
      val topout = PH.pathjoin(workdir, to + ".top")
      val tprout = PH.pathjoin(workdir, to + ".tpr")

      val cmd = List("grompp", "-f", "/mnt/nfs/" + from + ".mdp", "-c", groin, "-p", topin, "-pp", topout, "-o", tprout).mkString(" ")
      add(new GenericCmd(List(new File(topin), new File(groin), new File(posrein)), List(new File(topout), new File(tprout)), cmd))

    }

    (0 to 2).map(i => makegrom(stages(i), stages(i + 1)))

    def makemd(stage : String) {

      val tprin = PH.pathjoin(workdir, stage + ".tpr")
      val topin = PH.pathjoin(workdir, stage + ".top")
      val xtcout = PH.pathjoin(workdir, stage + ".xtc")
      val groout = PH.pathjoin(workdir, stage + ".gro")

      val cmd = List("mdrun", "-v", "-deffnm", stage)
      add(new GenericCmd(List(new File(tprin), new File(topin)), List(new File(xtcout), new File(groout))))

    }

    stages.map(makemd)

  }

}