package com.jwb.streamKDE

import breeze.stats.distributions.Gaussian
import com.twitter.algebird._
import CMSHasherImplicits._
import org.scalatest._
import matchers._

class KernelSpec extends FlatSpec with Matchers {

  def testMeanAndVariance(kernel: Kernel, firstMoment: Double, secondMoment: Double, tolerance: Double) ={
    val actualVariance = secondMoment - math.pow(firstMoment,2.0)

    (math.abs(kernel.mean() - firstMoment)/firstMoment) should be <= tolerance
    (math.abs(kernel.variance(0.0) - actualVariance)/actualVariance) should be <= tolerance
  }

  val testKernel1 = {
    val firstMoment = 10.0
    val secondMoment = 1000.0
    val count = 42
    val cmsMonoid = TopPctCMS.monoid[Long](.001, 1E-10, 1, .01)

    Kernel(AveragedValue(count, firstMoment), AveragedValue(count, secondMoment),
      cmsMonoid.create((1 to count).map(_ => 10L)), None)
  }

  val testKernel2 = {
    val firstMoment = 3.0
    val secondMoment = 25.0
    val count = 21
    val cmsMonoid = TopPctCMS.monoid[Long](.001, 1E-10, 1, .01)

    Kernel(AveragedValue(count, firstMoment), AveragedValue(count, secondMoment),
      cmsMonoid.create((1 to count).map(_ => 3L)), None)
  }

  val testKernel3 = {
    val firstMoment = 10.0
    val secondMoment = 100.0
    val count = 21
    val cmsMonoid = TopPctCMS.monoid[Long](.001, 1E-10, 1, .01)

    Kernel(AveragedValue(count, firstMoment), AveragedValue(count, secondMoment),
      cmsMonoid.create((1 to count).map(_ => 10L)), None)
  }

  "A Kernel" should "approximate mean and variance" in {
    val tolerance =   0.001

    val firstMoment = 10.0
    val secondMoment = 1000.0

    testMeanAndVariance(testKernel1, firstMoment, secondMoment, tolerance)
  }

  it should "use a default variance when its variance is too small" in {
    val alternativeVariance = 100.0

    testKernel3.variance(alternativeVariance) === alternativeVariance
  }

  it should "be able to be added to another Kernel" in{
    val tolerance = 0.001

    val firstMoment = 23.0/3
    val secondMoment = 2025.0/3

    val nuKernel = testKernel1 + testKernel2

    testMeanAndVariance(nuKernel, firstMoment, secondMoment, tolerance)
  }

  it should "be able to calculate its weight" in{
    val tolerance = 0.001

    val actualWeight = 42.0 / 30.0

    val approximatedWeight = testKernel1.weight(0.0)

    math.abs(approximatedWeight - actualWeight) / actualWeight should be <= tolerance
  }

  it should "be able to calculate its cdf value" in{
    val tolerance = 0.001

    val testVal = 10.0

    val actualGauss = Gaussian(10.0, 900.0)
    val actualCdf = actualGauss.cdf(testVal)

    val approxCdf = testKernel1.cdf(testVal)

    math.abs(approxCdf - actualCdf) / actualCdf should be <= tolerance
  }

  it should "be able to calculate its pdf value" in{
    val tolerance = 0.001

    val testVal = 10.0

    val actualGauss = Gaussian(10.0, 900.0)
    val actualPdf = actualGauss.pdf(testVal)

    val approxPdf = testKernel1.pdf(testVal)

    math.abs(approxPdf - actualPdf) / actualPdf should be <= tolerance
  }

  it should "be able to calculate its merge cost with another Kernel" in{
    val tolerance = 0.001

    val testVal = 10.0
    val g1 = (42.0 / 30.0) * Gaussian(10.0, 900.0).pdf(testVal)
    val g2 = (21.0/ 4.0) * Gaussian(3.0, 16.0).pdf(testVal)
    val g3Var = 2025.0/3 - math.pow(23.0/3,2.0)
    val g3 = (63.0/ math.sqrt(g3Var)) * Gaussian(23.0/3, g3Var).pdf(testVal)
    val actualMergeCost =  g1 + g2 - g3

    val approxMergeCost = testKernel1.mergeCostCalc(testKernel2, testVal).get

    math.abs(approxMergeCost - actualMergeCost) / actualMergeCost should be <= tolerance
  }

  it should "be able to generate a new Kernel with its new merge cost" in{
    val tolerance = 0.001

    val testVal = 10.0
    val g1 = (42.0 / 30.0) * Gaussian(10.0, 900.0).pdf(testVal)
    val g2 = (21.0/ 4.0) * Gaussian(3.0, 16.0).pdf(testVal)
    val g3Var = 2025.0/3 - math.pow(23.0/3,2.0)
    val g3 = (63.0/ math.sqrt(g3Var)) * Gaussian(23.0/3, g3Var).pdf(testVal)
    val actualMergeCost =  g1 + g2 - g3

    val approxMergeCost = testKernel1.updateWithMergeCost(testKernel2, testVal)

    approxMergeCost === testKernel1.copy(testKernel1.firstMoment, testKernel1.secondMoment, testKernel1.count, Some(actualMergeCost))
  }

}