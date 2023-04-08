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

package org.apache.arrow.adbc.driver.flightsql.dremio;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.arrow.adbc.core.*;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.adbc.driver.testsuite.SqlValidationQuirks;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Assumptions;

public class FlightSqlDremioQuirks extends SqlValidationQuirks {
  static final String FLIGHT_SQL_LOCATION_ENV_VAR = "ADBC_FLIGHT_SQL_LOCATION";
  static final String FLIGHT_SQL_USER_ENV_VAR = "ADBC_FLIGHT_SQL_USER";
  static final String FLIGHT_SQL_PASSWORD_ENV_VAR = "ADBC_FLIGHT_SQL_PASSWORD";

  static String getFlightLocation() {
    final String location = System.getenv(FLIGHT_SQL_LOCATION_ENV_VAR);
    Assumptions.assumeFalse(
        location == null || location.isEmpty(),
        "Flight SQL server not found, set " + FLIGHT_SQL_LOCATION_ENV_VAR);
    return location;
  }

  @Override
  public AdbcDatabase initDatabase(BufferAllocator allocator) throws AdbcException {
    String url = getFlightLocation();

    final Map<String, Object> parameters = new HashMap<>();
    parameters.put(AdbcDriver.PARAM_URL, url);
    parameters.put("adbc.username", System.getenv(FLIGHT_SQL_USER_ENV_VAR));
    parameters.put("adbc.password", System.getenv(FLIGHT_SQL_PASSWORD_ENV_VAR));
    return FlightSqlDriver.INSTANCE.open(parameters);
  }

  @Override
  public void cleanupTable(String name) throws Exception {
    try (final BufferAllocator allocator = new RootAllocator();
        final FlightSqlClient client =
            new FlightSqlClient(
                FlightClient.builder(allocator, new Location(getFlightLocation())).build())) {
      client.execute("DROP TABLE " + name);
    } catch (FlightRuntimeException e) {
      // Ignored
    }
  }

  @Override
  public String caseFoldTableName(String name) {
    return name;
  }

  @Override
  public String caseFoldColumnName(String name) {
    return name;
  }

  @Override
  public String defaultSchema() {
    return "adbc";
  }

  public void createTable(AdbcConnection connection,
                          String tableName,
                          VectorSchemaRoot root) {

    String signature = root.getSchema().getFields().stream()
            .map(Field::getName)
            .collect(Collectors.joining(","));

    ArrayList<String> rows = new ArrayList<String>();

    for (int i = 0; i < root.getRowCount(); i++) {
      int finalI = i;
      String row = root.getSchema().getFields().stream()
              .map(f -> {
                FieldVector vector = root.getVector(f.getName());
                boolean isNull = vector.isNull(finalI);

                if (Objects.equals(f.getFieldType().getType(), new ArrowType.Int(32, true))) {
                  if (isNull) {
                    return "CAST (NULL AS INT)";
                  } else {
                    return "CAST (" + ((IntVector) vector).get(finalI) + " AS INT)";
                  }

                } else if (Objects.equals(f.getFieldType().getType(), new ArrowType.Utf8())) {
                  if (isNull) {
                    return "CAST (NULL AS VARCHAR)";
                  } else {
                    return "'" + new String(((VarCharVector) vector).get(finalI), StandardCharsets.UTF_8) + "'";
                  }
                } else {
                  return "NULL";
                }
              }).collect(Collectors.joining(","));

      rows.add("(" + row + ")");
    }

    String query = "CREATE TABLE " + tableName + " as\n" +
            "SELECT *\n" +
            "FROM (\n" +
            "  VALUES " + String.join(",\n", rows) +
            ") AS T(" + signature + ")";

    try (final AdbcStatement stmt = connection.createStatement()) {
      stmt.setSqlQuery("DROP TABLE IF EXISTS " + tableName );
      stmt.executeQuery();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try (final AdbcStatement stmt = connection.createStatement()) {
      stmt.setSqlQuery(query);
      stmt.executeQuery();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // TODO Needs some time for changes to be applied. Maybe because we are using executeQuery instead of executeUpdate.
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
