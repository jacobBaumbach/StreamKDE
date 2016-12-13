package com.jwb.streamKDE

import breeze.stats.distributions.Gaussian
import collection.SortedMap
import com.twitter.algebird._
import CMSHasherImplicits._
import org.scalatest._
import matchers._

class KernelMapSpec extends FlatSpec with Matchers{
  val kernelGenerator = new KernelGenerator()
  val k1 = kernelGenerator.createKernel(1.0)
  val k2 = kernelGenerator.createKernel(2.0)
  val k3 = kernelGenerator.createKernel(3.0)

  val sortedMap = SortedMap((1.0, k1+k1+k1), (2.0, k2+k2+k2+k2))

  val undersizeKernelMap = new KernelMap(sortedMap, 3)

  val fullKernelMap = new KernelMap(sortedMap, 2)

  "A Kernel Map" should
    """when under size, and has a new, already seen,
      |value added to it should update the corresponding Kernel""".stripMargin in {

    val nuSortedMap = SortedMap((1.0, k1+k1+k1+k1), (2.0, k2+k2+k2+k2))

    val nuKernelMap = undersizeKernelMap + 1.0

    nuKernelMap.kMap === nuSortedMap
  }

  it should "when under size, and has a new, unseen, value added to it add a new Kernel" in {
    val nuSortedMap = SortedMap((1.0, k1+k1+k1), (2.0, k2+k2+k2+k2), (3.0, k3))

    val nuKernelMap = undersizeKernelMap + 3.0

    nuKernelMap.kMap === nuSortedMap
  }

  it should "is full, and has a new, already seen, value added to it should update the corresponding Kernel" in {
    val nuSortedMap = SortedMap((1.0, k1+k1+k1), (2.0, k2+k2+k2+k2+k2))

    val nuKernelMap = fullKernelMap + 2.0

    nuKernelMap.kMap === nuSortedMap
  }

  it should
    """is full, and has a new, unseen, value added to it should remove
      | the two Kernels with the smallest merge cost and add their merged result to the kMap""".stripMargin in {
    val nuSortedMap = SortedMap((1.0, k1+k1+k1), (2.6, k2+k2+k2+k2+k3))

    val nuKernelMap = fullKernelMap + 3.0

    nuKernelMap.kMap === nuSortedMap
  }

  it should "be able to tell you the frequency of a value" in {
    fullKernelMap.frequency(2) === 4L
  }

  it should "be able to calculate the cdf for a given value" in{
    val tolerance =  0.02

    val testArray = (1 to 10).map(i => (25.0*i, 12.5, .1)).toArray
    val (mgDraw, mgCdf, mgPdf) = KernelMap.multipleGaussian(testArray)
    val kMap = KernelMap.approx(mgDraw, 100000)

    val testVal = 100.0

    val actualCdf = mgCdf(testVal)

    val approxCdf = kMap.cdf(testVal)

    math.abs(approxCdf - actualCdf) / actualCdf should be <= tolerance
  }
}