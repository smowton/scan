import org.broadinstitute.sting.queue.QScript
import org.broadinstitute.sting.queue.function.{CommandLineFunction, JavaCommandLineFunction, InProcessFunction}
import org.broadinstitute.sting.queue.function.scattergather.ScatterGatherableFunction
import org.broadinstitute.sting.queue.extensions.gatk._
import org.broadinstitute.sting.gatk.walkers.genotyper.GenotypeLikelihoodsCalculationModel
import org.broadinstitute.sting.commandline.Output

import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.channels.Channels

trait ExtraArgs extends CommandLineFunction {

  var extraArgs : List[String] = _

  // This must be declared abstract to signify that this trait is not good enough
  // on its own to provide a commandLine -- it must be mixed in with some concrete
  // implementation, as otherwise it would try to call CommandLineFunction::commandLine
  // which is abstract.
  abstract override def commandLine : String = super.commandLine + repeat(prefix="", params=extraArgs, format="%s", escape=false)

}

trait Measure extends CommandLineGATK {

  def measureFile : String

  override def commandLine : String = {

    val scatterFactor = (
      if(this.isInstanceOf[ScatterGatherableFunction])
        this.asInstanceOf[ScatterGatherableFunction].scatterCount
      else
        1
    )

    return "expr $(stat -c %s " + measureFile + ") / \\( 1000000 \\* %d \\)".format(scatterFactor) + " > $SCAN_WORKCOUNT_FILE; " + super.commandLine

  }

}

trait MeasureReference extends Measure {

  override def measureFile : String = reference_sequence.getPath

}

trait MeasureInput extends Measure {

  override def measureFile : String = input_file(0).getPath

}

trait MeasureVariant extends Measure {

  override def measureFile : String = this.asInstanceOf[VariantFiltration].variant.getPath

}

trait OverrideTempDir extends JavaCommandLineFunction {

  // TODO fix the underlying class
  override def javaOpts : String = {

    val oldArgs = super.javaOpts
    val replace = "-Djava.io.tmpdir="
    val paramStart = oldArgs.indexOf(replace)

    if(paramStart == -1)
      return oldArgs

    val paramEnd = oldArgs.indexOf("'", paramStart + 1)
    if(paramEnd == -1)
      return oldArgs

    return oldArgs.substring(0, paramStart + replace.length) + jobLocalDir + oldArgs.substring(paramEnd)

  }

}

trait NeedsFastaIndex extends CommandLineFunction {

  override def inputs : Seq[File] = {

    val oldInputs = super.inputs
    val fastaInputs = oldInputs.filter(x => x.toString.endsWith(".fa") || x.toString.endsWith(".fasta"))
    val faiInputs = fastaInputs.map(x => new File(x.toString() + ".fai"))
    val dictInputs = fastaInputs.map(x => new File(x.toString().substring(0, x.toString().lastIndexOf('.')) + ".dict"))
    oldInputs ++ faiInputs ++ dictInputs

  }

}

class HttpFetch extends InProcessFunction {

  var url : String = _

  @Output
  var target : File = _

  def fetch(url : String, target : File) {

    val u = new URL(url)
    val rbc = Channels.newChannel(u.openStream())
    val fos = new FileOutputStream(target)
    fos.getChannel().transferFrom(rbc, 0, Long.MaxValue)

  }

  def run {

    fetch(url, target)
    fetch(url + ".bai", new File(target.getAbsolutePath() + ".bai"))

  }

}

class VarCallingPipeline extends QScript {

  def pathjoin(path1 : String, path2 : String) : String = new File(new File(path1), path2).getPath

  @Argument
  var input : String = _

  @Argument
  var workdir : String = _

  @Argument
  var refdir : String = _

  @Argument
  var estsize : String = _

  @Argument
  var scattercount : String = ""

  @Argument
  var singleQueue : Boolean = false

  def parseScatterCount : Seq[Int] = {

    if(scattercount == "") 
      return List.fill(7)(1) 

    val bits = scattercount.split(",")
    if(bits.length == 1)
      return List.fill(7)(bits(0).toInt)
    else if(bits.length == 7)
      return bits.map(x => x.toInt)
    else
      throw new Exception("scattercount must give one global count, or one for each of 7 phases")

  }

  def script {

    val genome = new File(pathjoin(refdir, "ref.fa"))
    val dbsnp = new File(pathjoin(refdir, "dbsnp.vcf"))
    //val indels = new File(pathjoin(refdir, "1kg.pilot_release.merged.indels.sites.hg19.human_g1k_v37.vcf"))

    val realignTargets = new File(pathjoin(workdir, "realign_targets.intervals"))
    val realignedBam = new File(pathjoin(workdir, "realigned.bam"))
    val recalData = new File(pathjoin(workdir, "recal.csv"))
    val recalBam = new File(pathjoin(workdir, "recal.bam"))
    val unfilteredCalls = new File(pathjoin(workdir, "unfiltered_calls.vcf"))
    val filteredCalls = new File(pathjoin(workdir, "filtered_calls.vcf"))
    val finalCalls = new File(pathjoin(workdir, "final_calls.vcf"))

    val scatterCounts = parseScatterCount

    def gatk_add(c : JavaCommandLineFunction, idx : Int) {

      // Received wisdom re: GATK and memory demand:
      c.javaMemoryLimit = Some(6)

      // Generic settings for GATK tasks:
      c.jobNativeArgs = List("estsize", estsize, "mempercore", "1")

      // Don't require the CWD to be accessible remotely:
      c.commandDirectory = workdir
      // Don't store temporary files on a network drive:
      c.jobLocalDir = "/tmp"

      if(singleQueue)
	c.jobQueue = "gatk_rtc"

      if(c.isInstanceOf[ScatterGatherableFunction]) {

        val sg = c.asInstanceOf[ScatterGatherableFunction]
        sg.scatterCount = scatterCounts(idx)

        // Redirect gather functions that execute out of process
        // to use a different task class, so as to expose the
        // cost of the gather stage.
        sg.setupGatherFunction = {
          case (x : CommandLineFunction, y) => { 
		if(!singleQueue)
			x.jobQueue = x.originalFunction.asInstanceOf[CommandLineFunction].jobQueue + "_gather"
	  }
        }

      }

      add(c)

    }

    val inputFile = 
      if(input.startsWith("http://")) {

        val target = new File(pathjoin(workdir, "in.bam"))
        val fetch = new HttpFetch
        fetch.url = input
        fetch.target = target
        add(fetch)
        target

      }
      else
        new File(input)

    val RTC = new RealignerTargetCreator with ExtraArgs with MeasureReference with OverrideTempDir with NeedsFastaIndex
    RTC.reference_sequence = genome
    RTC.input_file = List(inputFile)
    RTC.known = List(dbsnp)
    RTC.out = realignTargets
    RTC.jobQueue = "gatk_rtc"
    RTC.extraArgs = List("-nt", "$SCAN_CORES")
    
    gatk_add(RTC, 0)

    val IR = new IndelRealigner with MeasureInput with OverrideTempDir with NeedsFastaIndex
    IR.reference_sequence = genome
    IR.known = List(dbsnp)
    IR.input_file = List(inputFile)
    IR.targetIntervals = realignTargets
    IR.out = realignedBam
    IR.bam_compression = 0
    IR.jobQueue = "gatk_ir"

    gatk_add(IR, 1)

    val BR = new BaseRecalibrator with ExtraArgs with MeasureReference with OverrideTempDir with NeedsFastaIndex
    BR.reference_sequence = genome
    BR.input_file = List(realignedBam)
    BR.knownSites = List(dbsnp)
    BR.covariate = List("ReadGroupCovariate", "QualityScoreCovariate", "CycleCovariate", "ContextCovariate")
    BR.out = recalData
    BR.jobQueue = "gatk_br"
    BR.extraArgs = List("-nct", "$SCAN_CORES")

    gatk_add(BR, 2)

    val PR = new PrintReads with ExtraArgs with MeasureInput with OverrideTempDir with NeedsFastaIndex
    PR.reference_sequence = genome
    PR.input_file = List(realignedBam)
    PR.BQSR = recalData
    PR.out = recalBam
    PR.jobQueue = "gatk_pr"
    PR.extraArgs = List("-nct", "$SCAN_CORES")

    gatk_add(PR, 3)

    val UG = new UnifiedGenotyper with ExtraArgs with MeasureReference with OverrideTempDir with NeedsFastaIndex
    UG.reference_sequence = genome
    UG.input_file = List(recalBam)
    UG.glm = GenotypeLikelihoodsCalculationModel.Model.BOTH
    UG.stand_call_conf = 30.0
    UG.stand_emit_conf = 10.0
    UG.dbsnp = dbsnp
    UG.out = unfilteredCalls
    UG.jobQueue = "gatk_ug"
    UG.extraArgs = List("-nt", "$SCAN_CORES")

    gatk_add(UG, 4)

    val VF = new VariantFiltration with MeasureVariant with OverrideTempDir with NeedsFastaIndex
    VF.reference_sequence = genome
    VF.variant = unfilteredCalls
    VF.out = filteredCalls
    //VF.mask =
    VF.filterExpression = List("MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)", "QUAL < 30.0 || QD < 5.0")
    VF.filterName = VF.filterExpression.map(x => x.replace(">", ")").replace("<", "(").replace("=","_"))
    VF.clusterWindowSize = 10
    VF.jobQueue = "gatk_vf"

    gatk_add(VF, 5)

    val VE = new VariantEval with ExtraArgs with MeasureReference with OverrideTempDir with NeedsFastaIndex
    VE.reference_sequence = genome
    VE.dbsnp = dbsnp
    VE.eval = List(filteredCalls)
    VE.out = finalCalls
    VE.jobQueue = "gatk_ve"
    VE.extraArgs = List("-nt", "$SCAN_CORES")

    gatk_add(VE, 6)

  }

}
