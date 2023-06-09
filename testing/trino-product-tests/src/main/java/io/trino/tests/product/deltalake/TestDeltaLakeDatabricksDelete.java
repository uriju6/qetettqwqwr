/*
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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.testing.DataProviders;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Consumer;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_104;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_113;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestDeltaLakeDatabricksDelete
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeleteOnAppendOnlyTableFails()
    {
        String tableName = "test_delete_on_append_only_table_fails_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a INT, b INT)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "         TBLPROPERTIES ('delta.appendOnly' = true)");

        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2, 12)");
        assertQueryFailure(() -> onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1"))
                .hasMessageContaining("This table is configured to only allow appends");
        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1"))
                .hasMessageContaining("Cannot modify rows from a table with 'delta.appendOnly' set to true");

        assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                .containsOnly(row(1, 11), row(2, 12));
        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    // Databricks 12.1 added support for deletion vectors
    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectors()
    {
        String tableName = "test_deletion_vectors_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a INT, b INT)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "         TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2, 22)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11));

            // Reinsert the deleted row and verify that the row appears correctly
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (2, 22)");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));

            // Execute DELETE statement which doesn't delete any rows
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = -1");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));

            // Verify other statements
            assertThat(onTrino().executeQuery("SHOW TABLES FROM delta.default"))
                    .contains(row(tableName));
            assertThat(onTrino().executeQuery("SELECT column_name FROM delta.information_schema.columns WHERE table_schema = 'default' AND table_name = '" + tableName + "'"))
                    .contains(row("a"), row("b"));
            assertThat(onTrino().executeQuery("SELECT version, operation FROM delta.default.\"" + tableName + "$history\""))
                    .contains(row(0, "CREATE TABLE"), row(1, "WRITE"), row(2, "DELETE"));
            assertThat(onTrino().executeQuery("SHOW COLUMNS FROM delta.default." + tableName))
                    .contains(row("a", "integer", "", ""), row("b", "integer", "", ""));
            assertThat(onTrino().executeQuery("DESCRIBE delta.default." + tableName))
                    .contains(row("a", "integer", "", ""), row("b", "integer", "", ""));

            // TODO https://github.com/trinodb/trino/issues/17063 Use Delta Deletion Vectors for row-level deletes
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (3, 33)"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 7 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 7 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a = 3"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 7 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = -1"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 7 which is not supported");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsWithRandomPrefix()
    {
        String tableName = "test_deletion_vectors_random_prefix_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.randomizeFilePrefixes' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 11), (2, 22)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsWithCheckpointInterval()
    {
        String tableName = "test_deletion_vectors_random_prefix_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.checkpointInterval' = 1)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 11), (2, 22)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsMergeDelete()
    {
        String tableName = "test_deletion_vectors_merge_delete_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 10))");
            onDelta().executeQuery("MERGE INTO default." + tableName + " t USING default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED AND t.a > 5 THEN DELETE");

            List<Row> expected = ImmutableList.of(row(1), row(2), row(3), row(4), row(5));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsLargeNumbers()
    {
        String tableName = "test_deletion_vectors_large_numbers_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 10000))");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a > 1");

            List<Row> expected = ImmutableList.of(row(1));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS},
            dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsAcrossAddFile(boolean partitioned)
    {
        String tableName = "test_deletion_vectors_accross_add_file_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                (partitioned ? "PARTITIONED BY (a)" : "") +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2,22)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3,33), (4,44)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2 OR a = 4");

            List<Row> expected = ImmutableList.of(row(1, 11), row(3, 33));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).containsOnly(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(expected);

            // Verify behavior when the query doesn't read non-partition columns
            assertThat(onTrino().executeQuery("SELECT count(*) FROM delta.default." + tableName)).containsOnly(row(2));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsTruncateTable()
    {
        testDeletionVectorsDeleteAll(tableName -> onDelta().executeQuery("TRUNCATE TABLE default." + tableName));
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsDeleteFrom()
    {
        testDeletionVectorsDeleteAll(tableName -> onDelta().executeQuery("DELETE FROM default." + tableName));
    }

    private void testDeletionVectorsDeleteAll(Consumer<String> deleteRow)
    {
        String tableName = "test_deletion_vectors_delete_all_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 1000))");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).hasRowsCount(1000);

            deleteRow.accept(tableName);

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).hasNoRows();
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsOptimize()
    {
        String tableName = "test_deletion_vectors_optimize_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2,22)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3,33), (4,44)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1 OR a = 3");

            List<Row> expected = ImmutableList.of(row(2, 22), row(4, 44));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);

            onDelta().executeQuery("OPTIMIZE default." + tableName);

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsAbsolutePath()
    {
        String baseTableName = "test_deletion_vectors_base_absolute_" + randomNameSuffix();
        String tableName = "test_deletion_vectors_absolute_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + baseTableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + baseTableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + baseTableName + " VALUES (1,11), (2,22), (3,33), (4,44)");
            onDelta().executeQuery("DELETE FROM default." + baseTableName + " WHERE a = 1 OR a = 3");

            // The cloned table has 'p' (absolute path) storageType for deletion vector
            onDelta().executeQuery("CREATE TABLE default." + tableName + " SHALLOW CLONE " + baseTableName);

            List<Row> expected = ImmutableList.of(row(2, 22), row(4, 44));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).contains(expected);
            // TODO https://github.com/trinodb/trino/issues/17205 Fix below assertion when supporting absolute path
            assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .hasMessageContaining("Failed to generate splits");
        }
        finally {
            dropDeltaTableWithRetry("default." + baseTableName);
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsWithChangeDataFeed()
    {
        String tableName = "test_deletion_vectors_cdf_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.enableChangeDataFeed' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2,22), (3,33), (4,44)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1 OR a = 3");

            assertThat(onDelta().executeQuery(
                    "SELECT a, b, _change_type, _commit_version FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row(1, 11, "insert", 1L),
                            row(2, 22, "insert", 1L),
                            row(3, 33, "insert", 1L),
                            row(4, 44, "insert", 1L),
                            row(1, 11, "delete", 2L),
                            row(3, 33, "delete", 2L));

            // TODO Fix table_changes function failure
            assertQueryFailure(() -> onTrino().executeQuery("SELECT a, b, _change_type, _commit_version FROM TABLE(delta.system.table_changes('default', '" + tableName + "', 0))"))
                    .hasMessageContaining("Change Data Feed is not enabled at version 2. Version contains 'remove' entries without 'cdc' entries");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
