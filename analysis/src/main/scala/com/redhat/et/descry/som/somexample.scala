package com.redhat.et.descry.som

object Example {
  import org.apache.spark.mllib.linalg.{DenseVector => DV}
  import com.redhat.et.descry.som.SOM
  import com.redhat.et.descry.util.ImageWriter
  import org.apache.spark.SparkContext
  
  def apply(xdim: Int, ydim: Int, iterations: Int, sc: SparkContext, exampleCount: Int): SOM = {
    val rnd = new scala.util.Random()
    val colors = Array.fill(exampleCount)(new DV(Array.fill(3)(rnd.nextDouble)).compressed)
    val examples = sc.parallelize(colors)
    
    def writeStep(step: Int, som: SOM) {
      ImageWriter.write(xdim, ydim, som.entries, "som-step-%04d.png".format(step))
    }
    
    com.redhat.et.descry.som.SOM.train(xdim, ydim, 3, iterations, examples, sigmaScale=0.7, hook=writeStep _)
  }
  
}