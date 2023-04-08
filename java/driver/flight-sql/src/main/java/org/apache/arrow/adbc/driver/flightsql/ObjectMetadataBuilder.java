/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.adbc.driver.flightsql;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class ObjectMetadataBuilder {

  private final FlightSqlClient client;
  private final VectorSchemaRoot root;
  private final VarCharVector adbcCatalogNames;
  private final UnionListWriter adbcCatalogDbSchemasWriter;
  private final BufferAllocator allocator;
  private final AdbcConnection.GetObjectsDepth depth;
  private final String catalogPattern;
  private final String dbSchemaPattern;
  private final String tableNamePattern;
  private final String[] tableTypes;
  private final String columnNamePattern;

  ObjectMetadataBuilder(BufferAllocator allocator,
                        FlightSqlClient client,
                        final AdbcConnection.GetObjectsDepth depth,
                        final String catalogPattern,
                        final String dbSchemaPattern,
                        final String tableNamePattern,
                        final String[] tableTypes,
                        final String columnNamePattern
                        ) {
    this.allocator = allocator;
    this.client = client;
    this.depth = depth;
    this.catalogPattern = catalogPattern;
    this.dbSchemaPattern = dbSchemaPattern;
    this.tableNamePattern = tableNamePattern;
    this.tableTypes = tableTypes;
    this.columnNamePattern = columnNamePattern;
    this.root = VectorSchemaRoot.create(StandardSchemas.GET_OBJECTS_SCHEMA, allocator);
    this.adbcCatalogNames = (VarCharVector) root.getVector(0);
    this.adbcCatalogDbSchemasWriter = ((ListVector) root.getVector(1)).getWriter();

  }

  private void writeVarChar(VarCharWriter writer, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    try (ArrowBuf tempBuf = allocator.buffer(bytes.length)) {
      tempBuf.setBytes(0, bytes, 0, bytes.length);
      writer.writeVarChar(0, bytes.length, tempBuf);
    }
  }

  VectorSchemaRoot build() throws AdbcException {
    FlightInfo info = client.getTables(catalogPattern, dbSchemaPattern, tableNamePattern, tableTypes == null ? null : Arrays.asList(tableTypes),true);

    byte[] lastCatalogAdded = null;
    byte[] lastDbSchemaAdded = null;
    int catalogIndex = 0;

    for (FlightEndpoint endpoint : info.getEndpoints()) {
      FlightStream stream = client.getStream(endpoint.getTicket());
      while (stream.next()) {
        VectorSchemaRoot res = stream.getRoot();
        System.out.println(res.contentToTSVString());
        VarCharVector catalogVector = (VarCharVector) res.getVector(0);
        VarCharVector dbSchemaVector = (VarCharVector) res.getVector(1);

        BaseWriter.StructWriter catalogDbSchemasStructWriter = adbcCatalogDbSchemasWriter.struct();

        for (int i = 0; i < res.getRowCount(); i++) {
          byte[] catalog = catalogVector.get(i);
          byte[] dbSchema = dbSchemaVector.get(i);
          if (catalogIndex == 0 || lastCatalogAdded != catalog) {
            if (catalog == null) {
              adbcCatalogNames.setNull(catalogIndex);
            } else {
              adbcCatalogNames.set(catalogIndex, catalog);
            }
            if (depth == AdbcConnection.GetObjectsDepth.CATALOGS) {
              adbcCatalogDbSchemasWriter.writeNull();
            } else {
              if (catalogIndex != 0) {
                adbcCatalogDbSchemasWriter.endList();
              }
              adbcCatalogDbSchemasWriter.startList();
              lastDbSchemaAdded = null;
            }
            catalogIndex++;
            lastCatalogAdded = catalog;
          }

          if (!Arrays.equals(lastDbSchemaAdded, dbSchema)) {
            if (i != 0) {
              catalogDbSchemasStructWriter.list("db_schema_tables").endList();
              catalogDbSchemasStructWriter.end();
            }
            catalogDbSchemasStructWriter.start();
            writeVarChar(catalogDbSchemasStructWriter.varChar("db_schema_name"), new String(dbSchema, StandardCharsets.UTF_8));
            if (depth == AdbcConnection.GetObjectsDepth.CATALOGS || depth == AdbcConnection.GetObjectsDepth.DB_SCHEMAS) {
              catalogDbSchemasStructWriter.list("db_schema_tables").writeNull();
            } else {
              catalogDbSchemasStructWriter.list("db_schema_tables").startList();
            }

            lastDbSchemaAdded = dbSchema;
          }

          if (depth != AdbcConnection.GetObjectsDepth.CATALOGS && depth != AdbcConnection.GetObjectsDepth.DB_SCHEMAS) {
            BaseWriter.StructWriter dbSchemaTablesStructWriter = catalogDbSchemasStructWriter.list("db_schema_tables").struct();
            VarCharVector tableNameVector = (VarCharVector) res.getVector(2);
            VarCharVector tableTypeVector = (VarCharVector) res.getVector(3);

            dbSchemaTablesStructWriter.start();
            writeVarChar(dbSchemaTablesStructWriter.varChar("table_name"), new String(tableNameVector.get(i), StandardCharsets.UTF_8));
            writeVarChar(dbSchemaTablesStructWriter.varChar("table_type"), new String(tableTypeVector.get(i), StandardCharsets.UTF_8));
            dbSchemaTablesStructWriter.end();
          }
        }

        catalogDbSchemasStructWriter.list("db_schema_tables").endList();
        catalogDbSchemasStructWriter.end();
        adbcCatalogDbSchemasWriter.endList();
      }
    }

    this.root.setRowCount(catalogIndex);
    return root;
  }

}
