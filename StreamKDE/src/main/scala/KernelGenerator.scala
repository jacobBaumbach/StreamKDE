package com.jwb.streamKDE

import com.twitter.algebird._
import CMSHasherImplicits._

class KernelGenerator(delta: Double = 1E-10, eps: Double = 0.001, seed: Int = 1, heavyHittersPct: Double = 0.01){
  /*
   * KernelGenerator provides a way to create a Kernel with only a Double and ensures that all Kernels generated
   *  by KernelGenerator have their count min sketch initialized the same way
   *
   * NOTE: param descriptions pulled from
   * here: https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/CountMinSketch.scala
   * @param delta A bound on the probability that a query estimate does not lie within some small interval
   *              (an interval that depends on `eps`) around the truth.
   * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
   * @param seed A seed to initialize the random number generator used to create the pairwise independent
   *             hash functions.
   * @param heavyHittersPct A threshold for finding heavy hitters, i.e., elements that appear at least
   *                        (heavyHittersPct * totalCount) times in the stream.
   */

  //the initialized count min sketch to be used for all Kernels to be generated
  val cmsMonoid = TopPctCMS.monoid[Long](eps, delta, seed, heavyHittersPct)

  /*
   * createKernel generates an instance of Kernel with only a Double by initializing the first moment, second moment,
   *  and count with the appropriate values
   *
   * @param value the Double to have a Kernel wrapped around
   *
   * @return new instance of a Kernel based on value
   */
  def createKernel(value: Double): Kernel = {
    val firstMoment = AveragedValue(1L, value)
    val secondMoment = AveragedValue(1L, math.pow(value, 2.0))
    val count = cmsMonoid.create(List(math.round(value).toLong))

    Kernel(firstMoment, secondMoment, count, None)
  }

}