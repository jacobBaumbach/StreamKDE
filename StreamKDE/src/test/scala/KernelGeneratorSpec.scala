package com.jwb.streamKDE

import org.scalatest._
import matchers._

class KernelGeneratorSpec extends FlatSpec with Matchers{
  "A KernelGenerator" should "given two Doubles that are equal, generate two Kernels that are equal" in{
    val kernelGenerator = new KernelGenerator()

    val testVal = 10.0

    val k1 = kernelGenerator.createKernel(testVal)
    val k2 = kernelGenerator.createKernel(testVal)

    k1 === k2
  }
}