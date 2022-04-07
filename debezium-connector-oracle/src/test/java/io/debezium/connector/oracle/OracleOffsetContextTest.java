/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * Unit test that validates the behavior of the {@link OracleOffsetContext} and its friends.
 *
 * @author Chris Cranford
 */
public class OracleOffsetContextTest {

    private OracleConnectorConfig connectorConfig;
    private OffsetContext.Loader offsetLoader;

    @Before
    public void beforeEach() throws Exception {
        this.connectorConfig = new OracleConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = connectorConfig.getAdapter().getOffsetContextLoader();
    }

    @Test
    @FixFor("DBZ-2994")
    public void shouldreadScnAndCommitScnAsLongValues() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, 12345L);
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, 23456L);

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf("12345"));
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            assertThat(offsetContext.getCommitScn()).isEqualTo(Scn.valueOf("23456"));
        }
    }

    @Test
    @FixFor("DBZ-2994")
    public void shouldReadScnAndCommitScnAsStringValues() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, "12345");
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, "23456");

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf("12345"));
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            assertThat(offsetContext.getCommitScn()).isEqualTo(Scn.valueOf("23456"));
        }
    }

    @Test
    @FixFor("DBZ-2994")
    public void shouldHandleNullScnAndCommitScnValues() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, null);
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, null);

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isNull();
        assertThat(offsetContext.getCommitScn()).isNull();
    }

    @Test
    @FixFor("DBZ-4937")
    public void shouldCorrectlySerializeOffsetsWithSnapshotBasedKeysFromOlderOffsets() throws Exception {
        // Offsets from Debezium 1.8
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, "745688898023");
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, "745688898024");
        offsetValues.put("transaction_id", null);

        OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);

        // Write values out as Debezium 1.9
        Map<String, ?> writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(SourceInfo.SCN_KEY)).isEqualTo("745688898023");
        assertThat(writeValues.get(SourceInfo.COMMIT_SCN_KEY)).isEqualTo("745688898024");
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_PENDING_TRANSACTIONS_KEY)).isNull();
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_SCN_KEY)).isNull();

        // Simulate reloading of Debezium 1.9 values
        offsetContext = (OracleOffsetContext) offsetLoader.load(writeValues);

        // Write values out as Debezium 1.9
        writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(SourceInfo.SCN_KEY)).isEqualTo("745688898023");
        assertThat(writeValues.get(SourceInfo.COMMIT_SCN_KEY)).isEqualTo("745688898024");
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_PENDING_TRANSACTIONS_KEY)).isNull();
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_SCN_KEY)).isNull();
    }
}
