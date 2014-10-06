import org.broadinstitute.sting.queue.QScript
import org.broadinstitute.sting.queue.function.{CommandLineFunction, JavaCommandLineFunction}
import org.broadinstitute.sting.queue.extensions.gatk._
import org.broadinstitute.sting.gatk.walkers.genotyper.GenotypeLikelihoodsCalculationModel

import java.io.File

trait ExtraArgs extends CommandLineFunction {

  var extraArgs : List[String] = _

  // This must be declared abstract to signify that this trait is not good enough
  // on its own to provide a commandLine -- it must be mixed in with some concrete
  // implementation, as otherwise it would try to call CommandLineFunction::commandLine
  // which is abstract.
  abstract override def commandLine : String = super.commandLine + repeat(prefix="", params=extraArgs, format="%s", escape=false)

}

class VarCallingPipeline extends QScript {

  def pathjoin(path1 : String, path2 : String) : String = new File(new File(path1), path2).getPath

  val sharedFSRoot = "/mnt/nfs/"
  val refRoot = pathjoin(sharedFSRoot, "gatk/reference")
  val genome = new File(pathjoin(refRoot, "Homo_sapiens.GRCh37.56.dna.chromosomes_and_MT.fa"))
  val dbsnp = new File(pathjoin(refRoot, "dbsnp_138.hg19_with_b37_names.vcf"))
  val indels = new File(pathjoin(refRoot, "1kg.pilot_release.merged.indels.sites.hg19.human_g1k_v37.vcf"))

  @Argument
  var input : File = _

  @Argument
  var workdir : String = _

  def script {

    val realignTargets = new File(pathjoin(workdir, "realign_targets.intervals"))
    val realignedBam = new File(pathjoin(workdir, "realigned.bam"))
    val recalData = new File(pathjoin(workdir, "recal.csv"))
    val recalBam = new File(pathjoin(workdir, "recal.bam"))
    val unfilteredCalls = new File(pathjoin(workdir, "unfiltered_calls.vcf"))
    val filteredCalls = new File(pathjoin(workdir, "filtered_calls.vcf"))
    val finalCalls = new File(pathjoin(workdir, "final_calls.vcf"))

    def gatk_add(c : JavaCommandLineFunction) {
      c.javaMemoryLimit = Some(6)
      add(c)
    }

    val RTC = new RealignerTargetCreator with ExtraArgs
    RTC.reference_sequence = genome
    RTC.input_file = List(input)
    RTC.known = List(indels)
    RTC.out = realignTargets
    RTC.jobQueue = "gatk_rtc"
    RTC.extraArgs = List("-nt", "$SCAN_CORES")
    
    gatk_add(RTC)

    val IR = new IndelRealigner
    IR.reference_sequence = genome
    IR.known = List(indels)
    IR.input_file = List(input)
    IR.targetIntervals = realignTargets
    IR.out = realignedBam
    IR.bam_compression = 0
    IR.jobQueue = "gatk_ir"

    gatk_add(IR)

    val BR = new BaseRecalibrator
    BR.reference_sequence = genome
    BR.input_file = List(realignedBam)
    BR.knownSites = List(indels, dbsnp)
    BR.covariate = List("ReadGroupCovariate", "QualityScoreCovariate", "CycleCovariate", "ContextCovariate")
    BR.out = recalData
    BR.jobQueue = "gatk_br"

    gatk_add(BR)

    val PR = new PrintReads
    PR.reference_sequence = genome
    PR.input_file = List(realignedBam)
    PR.BQSR = recalData
    PR.out = recalBam
    PR.jobQueue = "gatk_pr"

    gatk_add(PR)

    val UG = new UnifiedGenotyper
    UG.reference_sequence = genome
    UG.input_file = List(recalBam)
    UG.glm = GenotypeLikelihoodsCalculationModel.Model.BOTH
    UG.stand_call_conf = 30.0
    UG.stand_emit_conf = 10.0
    UG.dbsnp = dbsnp
    UG.out = unfilteredCalls
    UG.jobQueue = "gatk_ug"

    gatk_add(UG)

    val VF = new VariantFiltration
    VF.reference_sequence = genome
    VF.variant = unfilteredCalls
    VF.out = filteredCalls
    VF.mask = indels
    VF.filterExpression = List("MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)", "QUAL < 30.0 || QD < 5.0")
    VF.filterName = VF.filterExpression.map(x => x.replace(">", ")").replace("<", "(").replace("=","_"))
    VF.clusterWindowSize = 10
    VF.jobQueue = "gatk_vf"

    gatk_add(VF)

    val VE = new VariantEval
    VE.reference_sequence = genome
    VE.dbsnp = dbsnp
    VE.eval = List(filteredCalls)
    VE.out = finalCalls
    VE.jobQueue = "gatk_ve"

    gatk_add(VE)

  }

}
