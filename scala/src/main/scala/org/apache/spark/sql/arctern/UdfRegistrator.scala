package org.apache.spark.sql.arctern

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.arctern.expressions._

object UdfRegistrator {
  def register(spark: SparkSession) = {
    // Register constructor UDFs
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_GeomFromText", ST_GeomFromText)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Point", ST_Point)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_PolygonFromEnvelope", ST_PolygonFromEnvelope)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_GeomFromGeoJSON", ST_GeomFromGeoJSON)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_AsText", ST_AsText)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_AsGeoJSON", ST_AsGeoJSON)
    // Register function UDFs
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Within", ST_Within)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Centroid", ST_Centroid)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_IsValid", ST_IsValid)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ST_IsSimple", ST_IsSimple)
  }
}