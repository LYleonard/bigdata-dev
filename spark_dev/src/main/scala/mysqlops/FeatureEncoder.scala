package mysqlops

import java.util.Properties

import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * @author
 * @date 2022/6/7 9:58
 * @description 数据预处理、归一化、One-hot编码
 */
object FeatureEncoder {

  case class Part(partkey:Long, mfgr:String, brand:String, size:Double, retailprice: Double)
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("FeatureMatrix").master("local[*]").getOrCreate()
    //隐式转换
    import sparkSession.implicits._
    val url = "jdbc:mysql://localhost:3306/dwd?characterEncoding=utf-8&serverTimezone=GMT%2B8"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "243015")

    val rawData = sparkSession.read.jdbc(url = url, table = "part", prop)
      .select($"PARTKEY".as("partkey"), $"MFGR".as("mfgr"),
        $"BRAND".as("brand"), $"SIZE".as("size"),
        $"RETAILPRICE".as("retailprice"))
      .filter($"mfgr" =!= "null" && $"brand" =!= "null" && $"size" =!= "null" && $"retailprice" =!= "null")

    val data = rawData.map(row=>{
      val partkey = row.getAs[Int]("partkey")
      val mfgr = row.getAs[String]("mfgr").replace("\"","")
      val brand = row.getAs[String]("brand").replace("\"","")
      val size = row.getAs[String]("size").replace("\"","")
      val retailprice = row.getAs[String]("retailprice").replace("\"","")
      Part(partkey, mfgr, brand, size.toDouble, retailprice.toDouble)
    })

    // 自定义UDF：构造单个元素的Vector
    def toVector: UserDefinedFunction = udf((size: Double) => {
      Vectors.dense(size)
    })
    // 自定义UDF：构造单个元素的Vector转为字符串
    def vectorToAny: UserDefinedFunction = udf((v: Vector) => {
      v.toArray.mkString
    })

    // MinMaxScaler 归一化
    val vectorDF =data.withColumn("sizeVector", toVector($"size")).withColumn("retailpriceVector",toVector($"retailprice"))
    val scalerSize = new MinMaxScaler().setInputCol("sizeVector").setOutputCol("scaledSize").fit(vectorDF).transform(vectorDF)
    val scalerSizePrice = new MinMaxScaler().setInputCol("retailpriceVector").setOutputCol("scaledRetailprice").fit(scalerSize).transform(scalerSize)
    val sizeDeScale = scalerSizePrice.withColumn("sizeDeScale",vectorToAny($"scaledSize"))
    val scalerDeSizePrice = sizeDeScale.withColumn("retailpriceDeScale",vectorToAny($"scaledRetailprice"))

    scalerDeSizePrice.show()

    // one-hot编码
    val indexMfgr = new StringIndexer().setInputCol("mfgr").setOutputCol("mfgrIndex").fit(scalerSizePrice)
    val indexedMfgr = indexMfgr.transform(scalerSizePrice)
    val indexBrand = new StringIndexer().setInputCol("brand").setOutputCol("brandIndex").fit(indexedMfgr)
    val indexedBrandMfgr = indexBrand.transform(indexedMfgr)
    val oneHotEncoder = new OneHotEncoderEstimator().setInputCols(Array("mfgrIndex", "brandIndex")).setOutputCols(Array("mfgrEncoder", "brandEncoder")).setDropLast(false).fit(indexedBrandMfgr)
    val encoded = oneHotEncoder.transform(indexedBrandMfgr)
    encoded.show()
    //    encoded.printSchema()

    val mfgr = encoded.select($"mfgr",$"mfgrIndex").distinct().orderBy($"mfgrIndex")
    val band = encoded.select($"brand",$"brandIndex").distinct().orderBy($"brandIndex")

    val mfgrSchema = StructType(mfgr.rdd.map(f=>StructField(f.get(0).toString,DoubleType,true)).collect().toList)
    val bandSchema = StructType(band.rdd.map(f=>StructField(f.get(0).toString,DoubleType,true)).collect().toList)
    val extendSchema = RowEncoder(StructType(encoded.schema ++ mfgrSchema ++ bandSchema))

    def extract(row: Row)={
      Row.merge(row, Row.fromSeq(row.getAs[Vector]("mfgrEncoder").toArray.toList),
        Row.fromSeq(row.getAs[Vector]("brandEncoder").toArray.toList))
    }

    val factPart = encoded.map(extract)(extendSchema).orderBy($"partkey", $"size")

    factPart.show()


    //    factPart.write.mode("append").format("Hive").insertInto("dwd.fact_part_machine_data")
    factPart.drop("sizeVector", "retailpriceVector", "scaledSize", "scaledRetailprice", "mfgrEncoder", "brandEncoder")
      .repartition(1).write.mode("append").option("header","true").csv("E:\\tmp\\files")


  }
}
