package com.jwb.streamKDE

import breeze.stats.distributions.Gaussian
import collection.SortedMap
import com.twitter.algebird._

class KernelMap(val kMap: SortedMap[Double, Kernel], val size: Int = 2000){
  /*
   * KernelMap is an implementation of a streaming Kernel Density Estimation algorithm
   * outlined here: http://alumni.cs.ucr.edu/~wli/publications/deosd.pdf
   *
   * KernelMap provides a way to approximate the distribution for a given stream of data using
   * size number of Kernels
   *
   * @param kMap a map sorted by key where the key is the approximated mean for the given Kernel, where the
   *             Kernel is the key
   * @param size the number of Kernels you want used to approximate the distribution
   */

  /*
   * this is an alternate constructor where you only need to specify the number of Kernels you
   * want to approximate the distribution
   *
   * @param size the number of Kernels you want used to approximate the distribution
   *
   * @return instance of KernelMap with kMap set to an empty SortedMap with size set to the specified amount
   */
  def this(size: Int) = {
    this(SortedMap[Double, Kernel](), size)
  }

  /*
   * this is an alternate constructor where no parameters are specified
   *
   * @return instance of KernelMap with kMap set to an empty SortedMap with size set to 1000
   */
  def this() = {
    this(SortedMap[Double, Kernel]())
  }

  // initializes an instance of KernelGenerator, so that all values of the stream can be wrapped in a Kernel
  // with their count min sketch initialzed with the same values
  val kernelGenerator = new KernelGenerator()

  // accumulates all the count min sketches to a single count min sketch so we can approximate the number of occurrences
  // for a value in the stream (rounded to its nearest Long)
  lazy val counter = {
    val Kernel(_ ,_, initial, _) = kMap(kMap.firstKey)
    kMap.drop(1).foldLeft(initial){case (acc, (_, Kernel(_ ,_, cm, _))) => acc ++ cm}
  }

  // accumulates all the Kernels' first and second moments
  // these two moments are used to approximate the total count and variance for the entire stream
  lazy val (totalFirstMoment, totalSecondMoment)  ={
    kMap.foldLeft((AveragedValue(0L, 0.0), AveragedValue(0L, 0.0))){
      case ((accFirst, accSecond), (_, Kernel(firstMomentI, secondMomentI, _, _))) =>
                                                                    (accFirst + firstMomentI, accSecond + secondMomentI)
    }
  }

  // approximation of the total number of values observed in the stream
  lazy val totalCount = totalFirstMoment.count

  // approximate variance for all the values observed in the stream using E(X^2) - (E(X)^2).
  // This value will be passed as the alternativeVaraince parameter for all instances of Kernel.
  // Hence, if a Kernel's variance is too small the approximate variance of the entire stream will
  // be used instead.
  lazy val alternativeVariance = totalSecondMoment.value - math.pow(totalFirstMoment.value, 2.0)

  /*
   * + is a way to add a new value from the stream to the kMap
   *
   * @param value item from the stream we want added to the kMap
   *
   * @return a new KernelMap instances with an updated kMap that includes value in its approximation
   */
  def +(value: Double): KernelMap = this.combine(kernelGenerator.createKernel(value), value)

  /*
   * combine is a way to add a Kernel to the kMap
   * following the procedure outlined here: http://alumni.cs.ucr.edu/~wli/publications/deosd.pdf
   *
   * @param kernel new Kernel we want added to the kMap
   * @param value item from the stream we want added to the kMap and will be used as the x for all the equations
   *              outlined here: http://alumni.cs.ucr.edu/~wli/publications/deosd.pdf
   *
   * @return a new KernelMap instances with an updated kMap that includes kernel in its approximation
   */
  def combine(kernel: Kernel, value: Double): KernelMap ={

    /*
     * updateMergeCost updates the merge cost for each Kernel between it and the Kernel with the largest mean that is
     *                 is less than current Kernel's mean.  The Kernel with the smallest mean has its merge cost set
     *                 to None.
     *
     * @param nuMap the SortedMap of Kernels which you would like the merge cost updated for
     *
     * @return a new SortedMap where each Kernel has had its merge cost updated
     */
    def updateMergeCost(nuMap: SortedMap[Double, Kernel]): SortedMap[Double, Kernel] = {
      val firstKey = nuMap.firstKey
      val kernelI= nuMap(firstKey)
      val initial = SortedMap[Double, Kernel]((firstKey,
                                                Kernel(kernelI.firstMoment, kernelI.secondMoment, kernelI.count, None)))

      nuMap.drop(1).foldLeft((initial, kernelI)){case ((accMap, prevKernel),(_, curKernel) ) =>
        val nuKernel = curKernel.updateWithMergeCost(prevKernel, value, alternativeVariance)
        (accMap + (nuKernel.mean() -> nuKernel), nuKernel)
      }._1
    }

    /*
     * minMergeCost finds the pair of Kernels that generated the smallest merge cost
     *
     * @param nuMap the SortedMap which we want to find the smallest merge cost for
     *
     * @return a tuple the first two values are the keys for the nuMap that correspond to the two Kernels
     *         that form the smallest merge cost and the final value is the nuMap itself
     */
    def minMergeCost(nuMap: SortedMap[Double, Kernel]): (Double, Double, SortedMap[Double, Kernel]) = {
      val meanI = nuMap.firstKey
      val Kernel(_, _, _, mergeCostI) = kMap(meanI)
      val (pMin, cMin) = nuMap.drop(1).foldLeft(((-1.0, meanI), mergeCostI, meanI)){
        case (((minPrev, minCur), mergeMin, prevMean), (meanCur, kernelCur)) =>
          (mergeMin, kernelCur.mergeCost) match{
            case (None, a) => ((prevMean, meanCur), a, meanCur)
            case (a, None) => ((minPrev, minCur), a, meanCur)
            case (Some(min), Some(cur)) if(min <= cur) => ((minPrev, minCur), Some(min), meanCur)
            case (Some(min), Some(cur)) if(min > cur) => ((prevMean, meanCur), Some(cur), meanCur)
        }
      }._1
      (pMin, cMin, nuMap)
    }

    /*
     * updateMerge removes the two Kernels that formed the smallest merge cost from the SortedMap, merges the two
     * Kernels that formed the smallest merge cost into the single Kernel and adds the resulting Kernel into the
     * SortedMap
     *
     * @param pMin the key to the first Kernel that generated the smallest merge cost
     * @param cMin the key to the second Kernel that generated the smallest merge cost
     * @param nuMap SortedMap for which we will remove the two Kernels and add the new merged Kernel
     *
     * @return the new SortedMap which the two Kernels were removed and the merged Kernel was added
     */
    def updateMerge(pMin: Double, cMin: Double, nuMap: SortedMap[Double, Kernel]): SortedMap[Double, Kernel] ={
      val pKernel = nuMap(pMin)
      val cKernel = nuMap(cMin)
      val nuKernel = pKernel + cKernel

      nuMap - pMin - cMin + (nuKernel.mean() -> nuKernel)
    }

    // check if there is already an existing Kernel in the kMap for the value
    val containValue = (this.kMap.contains(value))

    // check if kMap is smaller than the maximum size
    val underSize = (this.kMap.size < this.size)

    // case where there is already an existing Kernel in the kMap for the value
    // and kMap is smaller than the maximum size
    if(containValue && underSize){
      val oldKernel = this.kMap(value)
      val nuKernel = oldKernel + kernel
      val nuMap = this.kMap - value + (value -> nuKernel)
      new KernelMap(nuMap, this.size)
    }

    // case where the kMap is smaller than the maximum size
    // and there is NOT an existing Kernel in the kMap for the value
    else if(underSize) new KernelMap(this.kMap + (kernel.mean() -> kernel), this.size)

    // case where there is already an existing Kernel in the kMap for the value
    // and the kMap is LARGER than the maximum size
    else if(containValue){
      val oldKernel = this.kMap(value)
      val nuKernel = oldKernel + kernel
      val nuMap = updateMergeCost(this.kMap - value + (value -> nuKernel))
      new KernelMap(nuMap, this.size)
    }

    // case where kMap is LARGER than the maximum size
    // and there is NOT already an existing Kernel in the kMap for the value
    else{
      val updatedMapPF = updateMergeCost _ andThen minMergeCost _ andThen{case (a,b,c) =>
                                                                           updateMerge(a,b,c)} andThen updateMergeCost _
      val updatedMap = updatedMapPF(this.kMap)
      new KernelMap(updatedMap, this.size)
    }
  }

  /*
   * frequency gives the approximate number of occurrences for all the values that round to the Long specified
   *
   * @param value the Long you want to check the number of occurrences for
   *
   * @return the approximate number of occurrences for value
   */
  def frequency(value: Long): Long = counter.frequency(value).estimate

  /*
   * pdf gives the pdf value based on the approximated distribution for the stream based on the formula found
   * here: http://alumni.cs.ucr.edu/~wli/publications/deosd.pdf
   *
   * @param value the value we want to find the pdf value for
   *
   * @return the pdf value based on the approximated distribution for the specified input
   */
  def pdf(value: Double): Double = {
    (1.0/this.totalCount)*this.kMap.foldLeft(0.0){ case (acc, (k, v)) =>
                                                acc + (v.count.totalCount*v.pdf(value, alternativeVariance))}
  }

  /*
   * cdf gives the cdf value based on the approximated distribution for the stream
   *
   * @param value the value we want to find the cdf value for
   *
   * @return the cdf value based on the approximated distribution for the specified input
   */
  def cdf(value: Double): Double ={
    (1.0/this.totalCount)*this.kMap.foldLeft(0.0){case (acc, (k, v)) =>
                                                           acc + (v.count.totalCount*v.cdf(value, alternativeVariance))}
  }
}

object KernelMap{
  /*
   * Companion Object for KernelMap that provides the static method to generate a test stream and creates
   * an instance of the KernelMap based on that stream and another method to generate a draw, cdf and pdf
   * functions for a linear combination of Gaussians
   */

  /*
   * approx generate a test stream and creates an instance of the KernelMap based on that stream
   *
   * @param randomFunc is the function that will generate the random values for your stream
   * @param size the number of values you want in the stream
   * @param kernelMap the KernelMap you want to use to approximate the distribution of the stream
   *
   * @return a new instance of KernelMap that approximates the distribution for the test stream
   */
  def approx(randomFunc: () => Double, size: Int, km: KernelMap = new KernelMap()): KernelMap ={

    /*
     * randomStream generates the test stream
     *
     * @return test stream containing size number of random values generated by randomFunc
     */
    def randomStream() = (1 to size).map(_ => randomFunc())

    randomStream().foldLeft(km)(_+_)
  }

  /*
   * mvGaussian generates a function to draw a random value from a linear combination of Gaussians, 
   * a function to calculate the cdf for a linear combination of Gaussians, a function to calculate 
   * the pdf for a linear combination of Gaussians
   *
   * @param gaussianArray a array where the first value is the mean of the Gaussian,
   *                     the second value is the variance for the Gaussian and the third value
   *                     of the tuple is the scalar to multiply the given Gaussian by
   *
   * @return a tuple with a function to draw a random value from a linear combination of Gaussians, 
   *         a function to calculate the cdf for a linear combination of Gaussians, a function to calculate 
   *         the pdf for a linear combination of Gaussians
   */
  def multipleGaussian(gaussianArray: Array[(Double, Double,Double)]):
                                                    (() => Double, Double => Double, Double => Double) ={
    
    //rearranges the map so we draw from each Gaussian according to its scalar weight
    val distribution ={
      val (array, sumScalar) = gaussianArray.foldLeft((Array[(Double, Double, Double, Double)](), 0.0)){case ((acc,accSc), (m, v, sc)) =>
                                                      (acc :+ (m,sc, sc+accSc, v), sc+accSc)}
      array.sorted.foldLeft(SortedMap[Double,(Gaussian, Double)]()){case (acc, (m, sc, accSc, v)) =>
        acc + (accSc/sumScalar -> (Gaussian(m,v),sc/sumScalar))}
    }

    /*
     * draw pulls a random Gaussian, draws a random value from it and multiplies it by its corresonding 
     *      scalar
     *
     * @return a random value from the linear combination of Gaussians
     */
     def draw(): Double ={
      def randoNum(): Double = {
        val prob = scala.util.Random.nextDouble()
        distribution.dropWhile { case (k, v) => k < prob }.firstKey
      }
      val (gaussian, scalar) = distribution(randoNum())
      gaussian.draw() 
     }
    

    /*
     * df is a higher order function to generate the cdf and pdf functions
     *
     * @param pc function that calculates either the cdf or pdf value for the given Gaussian
     * @param x the value we want to find the 
     *
     */
    def df(pc: (Gaussian, Double) => Double)(x: Double): Double =
      distribution.foldLeft(0.0){case (acc, (k,(g,sc))) => acc + sc*pc(g,x)}

    //function to calculate the cdf for the linear combination of Gaussians
    val cdf = df{case (g,x) => g.cdf(x)} _

    //function to calculate the pdf for the linear combination of Gaussians
    val pdf = df{case (g,x) => g.pdf(x)} _

    (draw, cdf, pdf)
  }

}

