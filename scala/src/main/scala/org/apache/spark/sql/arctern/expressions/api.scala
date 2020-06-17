package org.apache.spark.sql.arctern.expressions

import org.apache.spark.sql.Column

object api {
  // Constructor UDF API
  def st_geomfromtext(wkt: Column): Column = Column {
    ST_GeomFromText(Seq(wkt.expr))
  }

  def st_point(x: Column, y: Column): Column = Column {
    ST_Point(Seq(x.expr, y.expr))
  }

  def st_polygonfromenvelope(xMin: Column, yMin: Column, xMax: Column, yMax:Column): Column = Column {
    ST_PolygonFromEnvelope(Seq(xMin.expr, yMin.expr, xMax.expr, yMax.expr))
  }

  def st_geomfromgeojson(json: Column): Column = Column {
    ST_GeomFromGeoJSON(Seq(json.expr))
  }

  def st_astext(geo: Column): Column = Column {
    ST_AsText(Seq(geo.expr))
  }

  def st_asgeojson(geo: Column): Column = Column {
    ST_AsGeoJSON(Seq(geo.expr))
  }

  // Function UDF API
  def st_within(left: Column, right: Column): Column = Column {
    ST_Within(Seq(left.expr, right.expr))
  }

  def st_centroid(geo: Column): Column = Column {
    ST_Centroid(Seq(geo.expr))
  }

  def st_isvalid(geo: Column): Column = Column {
    ST_IsValid(Seq(geo.expr))
  }

  def st_geometrytype(geo: Column): Column = Column {
    ST_GeometryType(Seq(geo.expr))
  }

  def st_issimple(geo: Column): Column = Column {
    ST_IsSimple(Seq(geo.expr))
  }

  def st_npoints(geo: Column): Column = Column {
    ST_NPoints(Seq(geo.expr))
  }

  def st_convexhull(geo: Column): Column = Column {
    ST_ConvexHull(Seq(geo.expr))
  }

  def st_area(geo: Column): Column = Column {
    ST_Area(Seq(geo.expr))
  }

  def st_length(geo: Column): Column = Column {
    ST_Length(Seq(geo.expr))
  }

  def st_hausdorffdistance(left: Column, right: Column): Column = Column {
    ST_HausdorffDistance(Seq(left.expr, right.expr))
  }

  def st_distance(left: Column, right: Column): Column = Column {
    ST_Distance(Seq(left.expr, right.expr))
  }

  def st_equals(left: Column, right: Column): Column = Column {
    ST_Equals(Seq(left.expr, right.expr))
  }

  def st_touches(left: Column, right: Column): Column = Column {
    ST_Touches(Seq(left.expr, right.expr))
  }

  def st_overlaps(left: Column, right: Column): Column = Column {
    ST_Overlaps(Seq(left.expr, right.expr))
  }

  def st_crosses(left: Column, right: Column): Column = Column {
    ST_Crosses(Seq(left.expr, right.expr))
  }

  def st_contains(left: Column, right: Column): Column = Column {
    ST_Contains(Seq(left.expr, right.expr))
  }

  def st_intersects(left: Column, right: Column): Column = Column {
    ST_Intersects(Seq(left.expr, right.expr))
  }

  def st_distancesphere(left: Column, right: Column): Column = Column {
    ST_DistanceSphere(Seq(left.expr, right.expr))
  }


  def st_transform(geo: Column, sourceCRSCode: Column, targetCRSCode: Column): Column = Column {
    ST_Transform(Seq(geo.expr, sourceCRSCode.expr, targetCRSCode.expr))
  }

  def st_makevalid(geo: Column): Column = Column {
    ST_MakeValid(Seq(geo.expr))
  }

  // Aggregate Function UDF API
  val st_union_aggr = new ST_Union_Aggr
  val st_envelope_aggr = new ST_Envelope_Aggr
}