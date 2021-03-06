import org.broadinstitute.sting.queue.QScript
import org.broadinstitute.sting.queue.function.{CommandLineFunction, JavaCommandLineFunction}
import org.broadinstitute.sting.queue.function.scattergather.ScatterGatherableFunction
import org.broadinstitute.sting.queue.extensions.gatk._
import org.broadinstitute.sting.gatk.walkers.genotyper.GenotypeLikelihoodsCalculationModel
import org.broadinstitute.sting.commandline.{Input, Output}

import java.io.File

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

trait OrderingHack {

  @Input(required=false)
  var orderingInput : File = _

  @Output
  var orderingOutput : File = _  

}

class VarCallingPipeline extends QScript {

  def pathjoin(path1 : String, path2 : String) : String = new File(new File(path1), path2).getPath

  val sharedFSRoot = "/mnt/nfs/"
  val refRoot = sharedFSRoot // pathjoin(sharedFSRoot, "")
  val genome = new File(pathjoin(refRoot, "Homo_sapiens.GRCh37.56.dna.chromosomes_and_MT.fa"))
  val dbsnp = new File(pathjoin(refRoot, "dbsnp_138.hg19_with_b37_names.vcf"))
  val indels = new File(pathjoin(refRoot, "1kg.pilot_release.merged.indels.sites.hg19.human_g1k_v37.vcf"))

  @Argument
  var input : File = _

  @Argument
  var workdir : String = _

  @Argument
  var fakedir : String = _

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

    val realignTargets = new File(pathjoin(workdir, "realign_targets.intervals"))
    val realignedBam = new File(pathjoin(workdir, "realigned.bam"))
    val recalData = new File(pathjoin(workdir, "recal.csv"))
    val recalBam = new File(pathjoin(workdir, "recal.bam"))
    val unfilteredCalls = new File(pathjoin(workdir, "unfiltered_calls.vcf"))
    val filteredCalls = new File(pathjoin(workdir, "filtered_calls.vcf"))
    val finalCalls = new File(pathjoin(workdir, "final_calls.vcf"))

    val scatterCounts = parseScatterCount

    val nullOut = new File("/dev/null")
    val fakeProducts = (1 to 7).map(x => new File(pathjoin(fakedir, "fake%d".format(x))))

    def gatk_add(c : JavaCommandLineFunction, idx : Int) {

      // Received wisdom re: GATK and memory demand:
      c.javaMemoryLimit = Some(6)

      // Don't require the CWD to be accessible remotely:
      c.commandDirectory = workdir

      if(singleQueue)
	c.jobQueue = "linux"

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

    val RTC = new RealignerTargetCreator with ExtraArgs with MeasureReference with OrderingHack
    RTC.reference_sequence = genome
    RTC.input_file = List(input)
    RTC.known = List(indels)
    RTC.out = nullOut
    RTC.jobQueue = "gatk_rtc"
    RTC.extraArgs = List("-nt", "$SCAN_CORES")
    RTC.orderingOutput = fakeProducts(0)
    
    gatk_add(RTC, 0)

    val IR = new IndelRealigner with MeasureInput with OrderingHack
    IR.reference_sequence = genome
    IR.known = List(indels)
    IR.input_file = List(input)
    IR.targetIntervals = realignTargets
    IR.out = nullOut
    IR.bam_compression = 0
    IR.jobQueue = "gatk_ir"
    IR.orderingInput = fakeProducts(0)
    IR.orderingOutput = fakeProducts(1)

    gatk_add(IR, 1)

    val BR = new BaseRecalibrator with ExtraArgs with MeasureReference with OrderingHack
    BR.reference_sequence = genome
    BR.input_file = List(realignedBam)
    BR.knownSites = List(indels, dbsnp)
    BR.covariate = List("ReadGroupCovariate", "QualityScoreCovariate", "CycleCovariate", "ContextCovariate")
    BR.out = nullOut
    BR.jobQueue = "gatk_br"
    BR.extraArgs = List("-nct", "$SCAN_CORES")
    BR.orderingInput = fakeProducts(1)
    BR.orderingOutput = fakeProducts(2)

    gatk_add(BR, 2)

    val PR = new PrintReads with ExtraArgs with MeasureInput with OrderingHack
    PR.reference_sequence = genome
    PR.input_file = List(realignedBam)
    PR.BQSR = recalData
    PR.out = nullOut
    PR.jobQueue = "gatk_pr"
    PR.extraArgs = List("-nct", "$SCAN_CORES")
    PR.orderingInput = fakeProducts(2)
    PR.orderingOutput = fakeProducts(3)

    gatk_add(PR, 3)

    val UG = new UnifiedGenotyper with ExtraArgs with MeasureReference with OrderingHack
    UG.reference_sequence = genome
    UG.input_file = List(recalBam)
    UG.glm = GenotypeLikelihoodsCalculationModel.Model.BOTH
    UG.stand_call_conf = 30.0
    UG.stand_emit_conf = 10.0
    UG.dbsnp = dbsnp
    UG.out = nullOut
    UG.jobQueue = "gatk_ug"
    UG.extraArgs = List("-nt", "$SCAN_CORES")
    UG.orderingInput = fakeProducts(3)
    UG.orderingOutput = fakeProducts(4)

    gatk_add(UG, 4)

    val VF = new VariantFiltration with MeasureVariant with OrderingHack
    VF.reference_sequence = genome
    VF.variant = unfilteredCalls
    VF.out = nullOut
    VF.mask = indels
    VF.filterExpression = List("MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)", "QUAL < 30.0 || QD < 5.0")
    VF.filterName = VF.filterExpression.map(x => x.replace(">", ")").replace("<", "(").replace("=","_"))
    VF.clusterWindowSize = 10
    VF.jobQueue = "gatk_vf"
    VF.orderingInput = fakeProducts(4)
    VF.orderingOutput = fakeProducts(5)

    gatk_add(VF, 5)

    val VE = new VariantEval with ExtraArgs with MeasureReference with OrderingHack
    VE.reference_sequence = genome
    VE.dbsnp = dbsnp
    VE.eval = List(filteredCalls)
    VE.out = nullOut
    VE.jobQueue = "gatk_ve"
    VE.extraArgs = List("-nt", "$SCAN_CORES")
    VE.orderingInput = fakeProducts(5)
    VE.orderingOutput = fakeProducts(6)

    gatk_add(VE, 6)

  }

}
