z

%sh
hadoop fs -cat spark-ML/labelsAndPreds/RF_numaccesses_50/part-00000 | head -10
hadoop fs -cat spark-ML/labelsAndPreds/RF_numaccesses_50_WEEK1/part-00000 | head -10
#hadoop fs -get spark-ML/labelsAndPreds/RF_numaccesses_50/part-00000 /afs/cern.ch/user/m/mmeoni/www/phd_unipi/labelsAndPreds.csv

//import org.apache.spark._

import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam

// Import classes for MLLib regression labeledpoint, vectors, decisiontree, decisiontree model, MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.util.MLUtils

// Area under the Curve and graphical study
import scala.util.Random
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

case class Popularity(week:Int, dataset_name:String, primary_dataset:String, processed_dataset:String, acquisition_era:String, processing_version:String, datatier:String, client_domain:String, username:String, server_domain:String, server_site:String, user_protocol:String, dataset_size:Double, numaccesses:Double, proctime:Double, readbytes:Double)

// function to parse input into Popularity class  
def parsePopularity(str: String): Popularity = {
  val line = str.split(",")
  Popularity(line(0).toInt, line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11), line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble)
}

// Create an RDD with 1-year data to be used for training the model
val textRDD = sc.textFile(inputdir)
textRDD.take(5).foreach(println)
textRDD.count()

val popularityRDD = textRDD.map(parsePopularity).cache()
popularityRDD.take(3)

// Create an RDD with new week of actual data
val textRDDweek = sc.textFile(inputdirWeek)
textRDDweek.take(5).foreach(println)
textRDDweek.count()

val popularityRDDweek = textRDDweek.map(parsePopularity).cache()
popularityRDDweek.take(3)

var index = Array(0,0,0,0,0,0,0,0,0,0,0)

var datasetMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.dataset_name).distinct.collect.foreach(x => { datasetMap += (x -> index(0)); index(0) += 1 })
index(0)
datasetMap.toString

var primarydatasetMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.primary_dataset).distinct.collect.foreach(x => { primarydatasetMap += (x -> index(1)); index(1) += 1 })
index(1)
primarydatasetMap.toString

var processeddatasetMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.processed_dataset).distinct.collect.foreach(x => { processeddatasetMap += (x -> index(2)); index(2) += 1 })
index(2)
processeddatasetMap.toString

var acquisitioneraMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.acquisition_era).distinct.collect.foreach(x => { acquisitioneraMap += (x -> index(3)); index(3) += 1 })
index(3)
acquisitioneraMap.toString

var processingversionMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.processing_version).distinct.collect.foreach(x => { processingversionMap += (x -> index(4)); index(4) += 1 })
index(4)
processingversionMap.toString

var datatierMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.datatier).distinct.collect.foreach(x => { datatierMap += (x -> index(5)); index(5) += 1 })
index(5)
datatierMap.toString

var clientdomainMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.client_domain).distinct.collect.foreach(x => { clientdomainMap += (x -> index(6)); index(6) += 1 })
index(6)
clientdomainMap.toString

//var usernameMap: Map[String, Int] = Map()
//popularityRDD.map(popularity => popularity.username).distinct.collect.foreach(x => { usernameMap += (x -> index(7)); index(7) += 1 })
//index(7)
//usernameMap.toString

var serverdomainMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.server_domain).distinct.collect.foreach(x => { serverdomainMap += (x -> index(8)); index(8) += 1 })
index(8)
serverdomainMap.toString

var serversiteMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.server_site).distinct.collect.foreach(x => { serversiteMap += (x -> index(9)); index(9) += 1 })
index(9)
serversiteMap.toString

var userprotocolMap: Map[String, Int] = Map()
popularityRDD.map(popularity => popularity.user_protocol).distinct.collect.foreach(x => { userprotocolMap += (x -> index(10)); index(10) += 1 })
index(10)
userprotocolMap.toString

// Defining the features array
val modelname = "RF"
val threshold_param = "numaccesses"
val threshold = 50  // <--- Luca (loop)
val mlprep = popularityRDD.map(popularity => {
  val week = popularity.week
  val primary_dataset = primarydatasetMap(popularity.primary_dataset) // category
  val processed_dataset = processeddatasetMap(popularity.processed_dataset) // category
  val acquisition_era = acquisitioneraMap(popularity.acquisition_era) // category
  val processing_version = processingversionMap(popularity.processing_version) // category
  val datatier = datatierMap(popularity.datatier) // category
  val client_domain = clientdomainMap(popularity.client_domain) // category
  val server_domain = serverdomainMap(popularity.server_domain) // category
  val server_site = serversiteMap(popularity.server_site) // category
  val user_protocol = userprotocolMap(popularity.user_protocol) // category
  //val username = usernameMap(popularity.username) // category ---> improve username parsing in EOS/AAA
  val dataset_size = popularity.dataset_size
  val ispopular = if (popularity.numaccesses.toDouble > threshold) 1.0 else 0.0 // AND proctime AND readbytes  // <<---- Luca (howto parameterize?)
  Array(ispopular.toDouble, week.toDouble, primary_dataset.toDouble, processed_dataset.toDouble, acquisition_era.toDouble, processing_version.toDouble, datatier.toDouble, client_domain.toDouble, /* username.toDouble, */ server_domain.toDouble, server_site.toDouble, user_protocol.toDouble, dataset_size.toDouble)
})
mlprep.take(3)

//Making LabeledPoint of features - this is the training data for the model
val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11))))
mldata.take(1)
mldata.count
//res7: Array[org.apache.spark.mllib.regression.LabeledPoint] = Array((0.0,[0.0,2.0,900.0,1225.0,6.0,385.0,214.0,294.0]))
// mldata0 is %85 not popular dataset
val mldata0 = mldata.filter(x => x.label == 0).randomSplit(Array(0.85, 0.15))(1)
mldata0.count
// mldata1 is %100 popular dataset
val mldata1 = mldata.filter(x => x.label != 0)
mldata1.count
// mldata2 is popular and not popular
val mldata2 = mldata0 ++ mldata1
mldata2.count
//  split mldata2 into training and test data
val splits = mldata2.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))
trainingData.count

// set ranges for 0=week, 1=primary_dataset, 2=processed_dataset, 3=acquisition_era, 4=processing_version, 5=datatier, 6=client_domain, 7=server_domain, 8=server_site, 9=user_protocol
var categoricalFeaturesInfo = Map[Int, Int]()
categoricalFeaturesInfo += (0 -> 53)
categoricalFeaturesInfo += (1 -> primarydatasetMap.size) // category
categoricalFeaturesInfo += (2 -> processeddatasetMap.size) // category
categoricalFeaturesInfo += (3 -> acquisitioneraMap.size) // category
categoricalFeaturesInfo += (4 -> processingversionMap.size) // category
categoricalFeaturesInfo += (5 -> datatierMap.size) // category
categoricalFeaturesInfo += (6 -> clientdomainMap.size) // category
categoricalFeaturesInfo += (7 -> serverdomainMap.size) // category
categoricalFeaturesInfo += (8 -> serversiteMap.size) // category
categoricalFeaturesInfo += (9 -> userprotocolMap.size) // category
// categoricalFeaturesInfo += (10 -> usernameMap.size) // category
// Add MAX for popularity.dataset_size

categoricalFeaturesInfo

val nTraining = trainingData.count()

// Same for all algos?
val numClasses = 2
// Defning values for the other parameters
val impurity = "gini"
val maxDepth = 9
val maxBins = 8000

// RandomForest
val numTrees = 10 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.

// SVM and LinearRegression
val numIterations = 50 // 100

// Prepare actual Data

val mlprepWeek = popularityRDDweek.map(popularity => {
  val week = popularity.week
  val primary_dataset   = if (primarydatasetMap.contains(popularity.primary_dataset)) { primarydatasetMap(popularity.primary_dataset) } else 0 // category
  val processed_dataset = if (processeddatasetMap.contains(popularity.processed_dataset)) { processeddatasetMap(popularity.processed_dataset) } else 0 // category
  val acquisition_era   = if (acquisitioneraMap.contains(popularity.acquisition_era)) { acquisitioneraMap(popularity.acquisition_era) } else 0 // category
  val processing_version= if (processingversionMap.contains(popularity.primary_dataset)) { processingversionMap(popularity.processing_version) } else 0 // category
  val datatier          = if (datatierMap.contains(popularity.processing_version)) { datatierMap(popularity.datatier) } else 0 // category
  val client_domain     = if (clientdomainMap.contains(popularity.client_domain)) { clientdomainMap(popularity.client_domain) } else 0 // category
  val server_domain     = if (serverdomainMap.contains(popularity.server_domain)) { serverdomainMap(popularity.server_domain) } else 0 // category
  val server_site       = if (serversiteMap.contains(popularity.server_site)) { serversiteMap(popularity.server_site) } else 0 // category
  val user_protocol     = if (userprotocolMap.contains(popularity.user_protocol)) { userprotocolMap(popularity.user_protocol) } else 0 // category
  //val username = usernameMap(popularity.username) // category ---> improve username parsing in EOS/AAA
  val dataset_size = popularity.dataset_size
  val ispopular = if (popularity.numaccesses.toDouble > threshold) 1.0 else 0.0 // AND proctime AND readbytes
  Array(ispopular.toDouble, week.toDouble, primary_dataset.toDouble, processed_dataset.toDouble, acquisition_era.toDouble, processing_version.toDouble, datatier.toDouble, client_domain.toDouble, /* username.toDouble, */ server_domain.toDouble, server_site.toDouble, user_protocol.toDouble, dataset_size.toDouble)
})
mlprepWeek.take(3)

val weekData = mlprepWeek.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11))))
weekData.take(1)
weekData.count

// TRAIN SEVERAL CLASSIFIERS

// Possible errors:
//    requirement failed: DecisionTree requires maxBins (= 32) to be at least as large as the number of values in each categorical feature, but categorical feature 2 has 954 values. Considering remove this and other categorical features with a large number of values, or add more training examples.

// Train a DecisionTree model ..................................................................................................
val modelDT = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

// Train a SupportVectorMachine model (takes a lot!) ...........................................................................
// It is a regression model and predictions are float, i.e. cannot directly compute FP, FN etc... !!!
val modelSVM = SVMWithSGD.train(trainingData, numIterations)
modelSVM.clearThreshold()

// Train a Logistic Regression model ...........................................................................................
val modelLogR = new LogisticRegressionWithLBFGS().setNumClasses(numClasses).run(trainingData)

// Train a GradientBoostedTrees model ..........................................................................................
// DefaultParams for Classification use LogLoss by default.
val boostingStrategy = BoostingStrategy.defaultParams("Classification")
boostingStrategy.numIterations = 10 // Note: Use more iterations in practice.
boostingStrategy.treeStrategy.numClasses = 2
boostingStrategy.treeStrategy.maxDepth = 5
boostingStrategy.treeStrategy.maxBins = maxBins
// Empty categoricalFeaturesInfo indicates all features are continuous.
boostingStrategy.treeStrategy.categoricalFeaturesInfo =categoricalFeaturesInfo
val modelGBT = GradientBoostedTrees.train(trainingData, boostingStrategy)

modelDT.toDebugString
modelLogR.toDebugString
modelRF.toDebugString
modelGBT.toDebugString

// 0=dofM 4=carrier 3=crsarrtime1  6=origin  

// Train a RandomForest model (BEST CLASSIFIER) .................................................................................
val modelRF = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

// Train Linear Regression model (only if regression!) ..........................................................................
val modelLR = LinearRegressionWithSGD.train(trainingData, numIterations);

// Score the Model
val model = modelRF

// model.clearThreshold()  <-- ???

testData.take(1)
weekData.take(1)
//res21: Array[org.apache.spark.mllib.regression.LabeledPoint] = Array((0.0,[18.0,6.0,900.0,1225.0,6.0,385.0,214.0,294.0]))

// Evaluate model on testData and compute test error (point is each item of LabeledPoint array). 
// Prediction aka Score
// Label aka Target 
// Alternative code: val prediction = model.predict(testData.map(_.features));
val labelsAndPreds = weekData.map { point =>
  val prediction = model.predict(point.features) 
  (point.label, prediction)
}
labelsAndPreds.take(10)

val nSamples = weekData.count()

val wrongPrediction = (labelsAndPreds.filter { case (label, prediction) => (label != prediction) })
println("wrongPredictions (FP + FN) = " + wrongPrediction.count())
println("ratioWrongPred. = " + wrongPrediction.count().toDouble / nSamples)

val TP = (labelsAndPreds.filter (r => ( r._1==1 && r._2==1) )) 
val TN = (labelsAndPreds.filter (r => ( r._1==0 && r._2==0) ))
val FP = (labelsAndPreds.filter { case (label, prediction) => (label==0 && prediction==1) })
val FN = (labelsAndPreds.filter (r => ( r._2==0 && r._1==1) ))
println("TP (Hit)  = " + TP.count())
println("TN        = " + TN.count())
println("FP (Alarm)= " + FP.count())
println("FN        = " + FN.count())

println("TPR (Recall/Sensitivity) = " + (TP.count.toDouble / (TP.count.toDouble + FN.count.toDouble)))
println("TNR (Specificity) = " + (TN.count.toDouble / (TN.count.toDouble + FP.count.toDouble)))
println("FPR = " + (1 - TNR))
println("FNR = " + (1 - TPR))
println("Accuracy = " + ((TP.count.toDouble + TN.count.toDouble) / nSamples))
println("Precision= " + (TP.count.toDouble / (TP.count.toDouble + FP.count.toDouble)))
println("F1 = " + ((2 * TP.count.toDouble) / ((2 * TP.count.toDouble) + FP.count.toDouble + FN.count.toDouble)))

// root mean squared error (RMSE) quantifies the model accuracy. The smaller it is, the more accurate the model is. But also need to be careful to avoid overfitting
println("RMSE = " + math.sqrt( labelsAndPreds.map { case(p,t) => math.pow((p-t),2) }.mean()) );

val metrics = new BinaryClassificationMetrics(labelsAndPreds, 100)

// Precision by threshold
val precision = metrics.precisionByThreshold
precision.foreach { case (t, p) => println(s"Threshold: $t, Precision: $p") }

val recall = metrics.recallByThreshold
println("Recall by threshold (TPR) = " + recall)
recall.take(recall.count.toInt)
recall.foreach { case (t, r) => println(s"Threshold: $t, Recall: $r") }

println("Precision-Recall Curve = " + metrics.pr)

// F-measure
val f1Score = metrics.fMeasureByThreshold(1)
f1Score.foreach { case (t, f) => println(s"Threshold: $t, F-score: $f, Beta = 1") }
val fScore = metrics.fMeasureByThreshold(0.5)
fScore.foreach { case (t, f) => println(s"Threshold: $t, F-score: $f, Beta = 0.5") }

// Compute thresholds used in ROC and PR curves
val thresholds = precision.map(_._1)

println("Area under precision-recall curve = " + metrics.areaUnderPR)
println("ROC Curve = " + metrics.roc)
println("Area under ROC = " + metrics.areaUnderROC())

// Print metrics
recall.collect()
PRC.collect()
f1Score.collect()
fScore.collect()
roc.collect()

// Write outputs
val prediction = model.predict(weekData.map(_.features))
prediction.collect()

labelsAndPreds.map(x => x._1 + "," + x._2).coalesce(1, true).saveAsTextFile("spark-ML/labelsAndPreds/" + modelname + "_" + threshold_param + "_" + threshold + "_WEEK1")

//roc.map(x => x._1 + "," + x._2).saveAsTextFile("spark-ML/roc/" + modelname + "_" + threshold_param + "_" + threshold)
