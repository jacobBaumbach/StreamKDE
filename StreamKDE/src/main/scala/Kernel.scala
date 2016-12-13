package com.jwb.streamKDE

import breeze.stats.distributions.Gaussian
import com.twitter.algebird._
import CMSHasherImplicits._

case class Kernel(firstMoment: AveragedValue, secondMoment: AveragedValue,
                  count: TopCMS[Long], mergeCost: Option[Double]){
  /*
   * Kernel is the individual unit that makes up a KernelMap
   * based on this paper: http://alumni.cs.ucr.edu/~wli/publications/deosd.pdf
   *
   * Kernel approximates the distribution of a subset of a stream with a Gaussian
   * where its mean and variance are approximated by the subset's values
   *
   * @param firstMoment approximation of the first moment of the Gaussian based on the subset of the values
   * @param secondMoment approximation of the second moment of the Gaussian based on the subset of the values
   * @param count count min sketch to approximate the count of each distinct value(where each value is rounded to
   *              to its nearest Long)
   * @param mergeCost the cost of merging one kernel with another wrapped in an Option
   *                  calculated using a formula found here: http://alumni.cs.ucr.edu/~wli/publications/deosd.pdf
   *                  intuitively the more similar two kernel's distributions are the smaller the merge cost
   */

  /*
   * + is the way of merging two kernels into a single kernel.
   *
   * @param kernel2 the Kernel to be merged with this instance of Kernel
   *
   * @return a new instance of Kernel containing the merged values
   */
  def +(kernel2: Kernel): Kernel ={
    val nuMeanSum = this.firstMoment + kernel2.firstMoment
    val nuVarSum = this.secondMoment + kernel2.secondMoment
    val nuCount = this.count ++ kernel2.count
    Kernel(nuMeanSum , nuVarSum, nuCount, None)
  }

  /*
   * mean approximates the mean of the subset of values using the first moment
   *
   * @return approximated mean
   */
  def mean(): Double = this.firstMoment.value

  /*
   * variance approximates the variance of the subset of values using E(X^2) - (E(X)^2)
   *
   * @param alternativeVariance call by name double to be used as variance if the Kernel's approximated
   *                            variance is too small.  Primarily intended to avoid the cold start problem when
   *                            the Kernel only contains a few values
   *
   * @result approximated variance
   */
  def variance(alternateVariance: => Double = 1.0): Double = {
    val variant = this.secondMoment.value - math.pow(mean(), 2.0)
    if(variant <= 0.001) alternateVariance else variant
  }

  /*
   * weight generates the constant to multiply the pdf value by when calculating the merge cost or when generating
   * the pdf value for a KernelMap
   *
   * @param alternativeVariance call by name double to be used as variance if the Kernel's approximated
   *                            variance is too small.
   *
   * @return constant to multiply the pdf value
   */
  def weight(alternateVariance: => Double = 1.0): Double =
    this.count.totalCount / math.sqrt(this.variance(alternateVariance))

  /*
   * pdf generates the pdf value from the gaussian used to approximate the distribution for this subset of data
   *
   * @param x the value which a pdf value will be generated for
   * @param alternativeVariance call by name double to be used as variance if the Kernel's approximated
   *                            variance is too small.
   *
   * @return pdf value for x
   */
  def pdf(x: Double, alternateVariance: => Double = 1.0): Double = {
    Gaussian(mean(), variance(alternateVariance)).pdf(x)
  }

  /*
   * cdf generates the cdf value from the gaussian used to approximate the distribution for this subset of data
   *
   * @param x the value which a cdf value will be generated for
   * @param alternativeVariance call by name double to be used as variance if the Kernel's approximated
   *                            variance is too small.
   *
   * @return cdf value for x
   */
  def cdf(x: Double, alternateVariance: => Double = 1.0): Double ={
    Gaussian(mean(), math.sqrt(variance(alternateVariance))).cdf(x)
  }

  /*
   * mergeCostCalc calculates the merge cost between this kernel and another kernel for a specific value
   *  Merge cost approximates the similarity between two kernel's gaussian's.  The more similar two kernel's
   *  distribution's are the smaller the merge cost
   *
   * @param kernel2 the other kernel that will be used to calculate the merge cost
   * @param x the specific value for which the merge cost will be calculated for
   * @param alternativeVariance call by name double to be used as variance if the Kernel's approximated
   *                            variance is too small.
   *
   * @return merge cost between this Kernel and kernel2 at x wrapped in an Option
   */
  def mergeCostCalc(kernel2: Kernel, x: Double, alternateVariance: => Double = 1.0): Option[Double] ={
    val mergedKernel = this + kernel2

    val thisWeightedPdf = this.weight(alternateVariance) * this.pdf(x, alternateVariance)
    val kernel2WeightedPdf = kernel2.weight(alternateVariance) * kernel2.pdf(x, alternateVariance)
    val mergedWeightedPdf = mergedKernel.weight(alternateVariance) * mergedKernel.pdf(x, alternateVariance)

    val linearComboPdf = thisWeightedPdf + kernel2WeightedPdf - mergedWeightedPdf
    Some(linearComboPdf)
  }

  /*
   * updateWithMergeCost generates a new Kernel with all the values of this Kernel except the mergecost is updated
   *  with the merge cost between this kernel and another kernel evaluated at a specific value
   *
   * @param kernel2 the other kernel that will be used to calculate the merge cost
   * @param x the specific value for which the merge cost will be calculated for
   * @param alternativeVariance call by name double to be used as variance if the Kernel's approximated
   *                            variance is too small.
   *
   * @return new Kernel with the updated merge cost field
   */
  def updateWithMergeCost(kernel2: Kernel, x: Double, alternateVariance: => Double = 1.0): Kernel =
    Kernel(this.firstMoment, this.secondMoment, this.count, this.mergeCostCalc(kernel2, x, alternateVariance))

}
