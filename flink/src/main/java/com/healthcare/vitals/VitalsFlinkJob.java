/*
 * VitalsFlinkJob — executes the SQL pipeline from classpath:/job.sql.
 *
 * Deployment model: Managed Flink (Floci) pulls a JAR from local S3; the JAR's
 * main() builds a TableEnvironment and runs the DDL+DML statements. SQL lives
 * in flink/sql/job.sql (the make build-flink target copies it into resources).
 * Floci injects runtime overrides at /etc/flink/application_properties.json.
 */
package com.healthcare.vitals;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public final class VitalsFlinkJob {

    private static final Logger LOG = Logger.getLogger(VitalsFlinkJob.class.getName());
    private static final String SQL_RESOURCE = "/job.sql";
    // Set via ApplicationProperties when running under Managed Flink (Floci).
    private static final String ENDPOINT_ENV = "FLINK_AWS_ENDPOINT_URL";

    private VitalsFlinkJob() {
    }

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig().getConfiguration().setString("table.exec.resource.default-parallelism", "1");
        // Iceberg's FilesCommitter only publishes data on checkpoints; without an
        // interval the sink holds files indefinitely (visible empty tables).
        tableEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "10000ms");

        String endpoint = System.getenv(ENDPOINT_ENV);
        if (endpoint != null && !endpoint.isBlank()) {
            // Kinesis connector endpoint override so the SQL reads resolve to Floci.
            tableEnv.getConfig().getConfiguration().setString("flink.stream.kinesis.endpoint", endpoint);
        }

        try (InputStream in = VitalsFlinkJob.class.getResourceAsStream(SQL_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + SQL_RESOURCE);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            StringBuilder sql = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sql.append(line).append('\n');
            }
            executeSqlStatements(tableEnv, sql.toString());
        }
        LOG.info("vitals flink job finished statement submission");
    }

    /**
     * Splits and executes the pipeline; DDLs then one/few DML statements that
     * run as a single streaming job graph.
     */
    private static void executeSqlStatements(TableEnvironment tableEnv, String sql) {
        for (String statement : splitStatements(sql)) {
            if (statement.isBlank()) {
                continue;
            }
            LOG.info("executing:\n" + statement);
            tableEnv.executeSql(statement);
        }
    }

    private static java.util.List<String> splitStatements(String sql) {
        java.util.List<String> statements = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inLineComment = false;
        boolean inBlockComment = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';
            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    current.append('\n');
                }
                continue;
            }
            if (inBlockComment) {
                if (c == '*' && next == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }
            if (c == '-' && next == '-') {
                inLineComment = true;
                i++;
                continue;
            }
            if (c == '/' && next == '*') {
                inBlockComment = true;
                i++;
                continue;
            }
            if (c == ';') {
                statements.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        statements.add(current.toString());
        return statements;
    }
}