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

class CPScript extends QScript {

  @Argument
  var indir : String = _

  @Argument
  var outdir : String = _

  @Argument
  var tmpdir : String = _

  @Argument
  var estsize : String = _
 
  def cpadd(c : CommandLineFunction, classname : String) {

    c.jobNativeArgs = List("estsize", estsize, "mempercore", "1")
    c.commandDirectory = tmpdir
    c.jobLocalDir = "/tmp"
    c.jobQueue = classname
    c.nCoresRequest = Some(1)
    
    add(c)

  }

  def script {

    val infiles = (0 to 2).map(x => PH.pathjoin(indir, "IN%d.tif".format(x)));
    val interfiles = List(PH.pathjoin(outdir, "IN0_Outline.png"));
    val outfiles = List(PH.pathjoin(outdir, "Outline_thumbnail.png"));

    cpadd(new GenericCmd(infiles, interfiles, "/root/scan/cp_scripts/runcp.sh /home/user %s %s %s".format(indir, outdir, tmpdir)), "cpmain");
    cpadd(new GenericCmd(interfiles, outfiles, "imagemagick %s -resize 50x50 %s".format(interfiles(0), outfiles(0))), "cpthumb");

  }

}
