/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.calcite.parser;

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Extends the Insert call to hold custom paramaters specific to druid i.e. PARTITIONED BY and CLUSTERED BY
 * This class extends the {@link SqlInsert} so that this SqlNode can be used in
 * {@link org.apache.calcite.sql2rel.SqlToRelConverter} for getting converted into RelNode, and further processing
 */
public class DruidSqlInsert extends SqlInsert
{
  // This allows reusing super.unparse
  public static final SqlOperator OPERATOR = SqlInsert.OPERATOR;

  private final SqlNode partitionedBy;
  private final SqlNodeList clusteredBy;

  public DruidSqlInsert(
      @Nonnull SqlInsert insertNode,
      @Nullable SqlNode partitionedBy,
      @Nullable SqlNodeList clusteredBy
  )
  {
    super(
        insertNode.getParserPosition(),
        (SqlNodeList) insertNode.getOperandList().get(0), // No better getter to extract this
        insertNode.getTargetTable(),
        insertNode.getSource(),
        insertNode.getTargetColumnList()
    );
    this.partitionedBy = partitionedBy;
    this.clusteredBy = clusteredBy;
  }

  @Nullable
  public SqlNodeList getClusteredBy()
  {
    return clusteredBy;
  }

  @Nullable
  public String getPartitionedBy()
  {
    if (partitionedBy == null) {
      return null;
    }
    return SqlLiteral.unchain(partitionedBy).toValue();
  }

  @Nonnull
  @Override
  public SqlOperator getOperator()
  {
    return OPERATOR;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    super.unparse(writer, leftPrec, rightPrec);
    if (partitionedBy != null) {
      writer.keyword("PARTITIONED BY");
      writer.keyword(getPartitionedBy());
    }
    if (clusteredBy != null) {
      writer.sep("CLUSTERED BY");
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlNode clusterByOpts : getClusteredBy().getList()) {
        clusterByOpts.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(frame);
    }
  }

}
