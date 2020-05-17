/*
 * Copyright (C) 2019-2020 Zilliz. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.arctern.expressions

import org.apache.spark.sql.arctern.{ArcternExpr, CodeGenUtil, GeometryUDT}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._

abstract class ST_BinaryOp(f: (String, String) => String) extends ArcternExpr {

  def leftExpr: Expression

  def rightExpr: Expression

  override def nullable: Boolean = leftExpr.nullable || rightExpr.nullable

  override def eval(input: InternalRow): Any = {
    throw new RuntimeException("call implement method")
  }

  override def children: Seq[Expression] = Seq(leftExpr, rightExpr)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    assert(CodeGenUtil.isGeometryExpr(leftExpr))
    assert(CodeGenUtil.isGeometryExpr(rightExpr))

    var leftGeo : String = ""
    var leftGeoDeclare : String = ""
    var leftGeoCode : String = ""
    var rightGeo : String = ""
    var rightGeoDeclare : String = ""
    var rightGeoCode : String = ""

    val leftCode = leftExpr.genCode(ctx)
    val rightCode = rightExpr.genCode(ctx)

    if(CodeGenUtil.isArcternExpr(leftExpr)){
      val (geo, declare, code) = CodeGenUtil.extractGeometryConstructor(leftCode.code.toString())
      leftGeo = geo; leftGeoDeclare = declare; leftGeoCode = code
    }

    if(CodeGenUtil.isArcternExpr(rightExpr)){
      val (geo, declare, code) = CodeGenUtil.extractGeometryConstructor(rightCode.code.toString())
      rightGeo = geo; rightGeoDeclare = declare; rightGeoCode = code
    }

    if (nullable) {
      val nullSafeEval =
        leftGeoCode + ctx.nullSafeExec(leftExpr.nullable, leftCode.isNull) {
          rightGeoCode + ctx.nullSafeExec(rightExpr.nullable, rightCode.isNull) {
            s"""
               |${ev.isNull} = false; // resultCode could change nullability.
               |${ev.value} = ${f(leftGeo, rightGeo)};
               |""".stripMargin
          }
        }
      ev.copy(code =
        code"""
            boolean ${ev.isNull} = true;
            $leftGeoDeclare
            $rightGeoDeclare
            ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            $nullSafeEval
            """)

    }
    else {
      ev.copy(code =
        code"""
          $leftGeoDeclare
          $rightGeoDeclare
          $leftGeoCode
          $rightGeoCode
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          ${ev.value} = ${f(leftGeo, rightGeo)};
          """, FalseLiteral)
    }
  }

}

abstract class ST_UnaryOp(f: String => String) extends ArcternExpr {

  def inputExpr: Expression

  override def nullable: Boolean = inputExpr.nullable

  override def eval(input: InternalRow): Any = {
    throw new RuntimeException("call implement method")
  }

  override def children: Seq[Expression] = Seq(inputExpr)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val inputCode = inputExpr.genCode(ctx)

    val (inputGeo, inputGeoDeclare, inputGeoCode) = CodeGenUtil.extractGeometryConstructor(inputCode.code.toString())

    val assignment: String = dataType match {
      case _: GeometryUDT =>
        s"""
           |${CodeGenUtil.mutableGeometryInitCode(s"${ev.value}_geo")}
           |${ev.value}_geo = ${f(inputGeo)};
           |${ev.value} = ${CodeGenUtil.serialGeometryCode(s"${ev.value}_geo")}
           |""".stripMargin
      case _ => s"${ev.value} = ${f(inputGeo)};"
    }

    if (nullable) {
      val nullSafeEval =
        inputGeoCode + ctx.nullSafeExec(inputExpr.nullable, inputCode.isNull) {
          s"""
             |${ev.isNull} = false; // resultCode could change nullability.
             |$assignment
             |""".stripMargin
        }
      ev.copy(code =
        code"""
            boolean ${ev.isNull} = true;
            $inputGeoDeclare
            ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            $nullSafeEval
            """)
    } else {
      ev.copy(code =
        code"""
            $inputGeoDeclare
            $inputGeoCode
            ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            $assignment
            """, FalseLiteral)
    }
  }

}


case class ST_Within(inputsExpr: Seq[Expression])
  extends ST_BinaryOp((left, right) => s"$left.within($right)") {
  assert(inputsExpr.length == 2)

  override def leftExpr: Expression = inputsExpr(0)

  override def rightExpr: Expression = inputsExpr(1)

  override def dataType: DataType = BooleanType
}

case class ST_Centroid(inputsExpr: Seq[Expression])
  extends ST_UnaryOp(geo => s"$geo.getCentroid()") {

  assert(inputsExpr.length == 1)

  override def inputExpr: Expression = inputsExpr(0)

  override def dataType: DataType = new GeometryUDT

}
