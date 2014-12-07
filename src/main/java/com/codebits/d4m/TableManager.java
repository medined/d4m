package com.codebits.d4m;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.io.Text;

public class TableManager {

    @Setter
    @Getter
    private String baseTableName = "edge";
    
    @Getter
    @Setter
    private Connector connector = null;

    @Setter
    @Getter
    private TableOperations tableOperations = null;

    private final static Text EMPTY_CQ = new Text("");
    private final static Text FIELD_DELIMITER_PROPERTY_NAME = new Text("field.delimiter");
    private final static Text FACT_DELIMITER_PROPERTY_NAME = new Text("fact.delimiter");
    private final static Text PROPERTY = new Text("property");
    private final Charset charset = Charset.defaultCharset();

    public TableManager() {
    }

    public TableManager(final Connector connector, final TableOperations tableOperations) {
        this.connector = connector;
        this.tableOperations = tableOperations;
    }

    public void createTables(final String baseTableName) {
        setBaseTableName(baseTableName);
        createTables();
    }

    public void createTables() {
        Validate.notNull(connector, "connector must not be null");
        Validate.notNull(tableOperations, "tableOperations must not be null");

        /*
         * This code sets the default values. If you want to change them,
         * you can over-write the metadata entries instead of changing
         * the values here.
         */
        Value defaultFieldDelimiter = new Value("\t".getBytes(charset));
        Value defaultFactDelimiter = new Value("|".getBytes(charset));

        int isEdgePresent = tableOperations.exists(getEdgeTable()) ? 1 : 0;
        int isTransposePresent = tableOperations.exists(getTransposeTable()) ? 1 : 0;
        int isDegreePresent = tableOperations.exists(getDegreeTable()) ? 1 : 0;
        int isMetatablePresent = tableOperations.exists(getMetadataTable()) ? 1 : 0;
        int isTextPresent = tableOperations.exists(getTextTable()) ? 1 : 0;

        int tableCount = isEdgePresent + isTransposePresent + isDegreePresent + isMetatablePresent + isTextPresent;

        if (tableCount > 0 && tableCount < 5) {
            throw new D4MException("D4M: BASE[" + getBaseTableName() + "] Inconsistent state - one or more D4M tables is missing.");
        }

        if (tableCount == 5) {
            // assume the tables are correct.
            return;
        }

        try {
            tableOperations.create(getEdgeTable());
            tableOperations.create(getTransposeTable());
            tableOperations.create(getDegreeTable());
            tableOperations.create(getMetadataTable());
            tableOperations.create(getTextTable());

            IteratorSetting degreeIteratorSetting = new IteratorSetting(7, SummingCombiner.class);
            SummingCombiner.setEncodingType(degreeIteratorSetting, LongCombiner.Type.STRING);
            SummingCombiner.setColumns(degreeIteratorSetting, Collections.singletonList(new IteratorSetting.Column("", "degree")));
            tableOperations.attachIterator(getDegreeTable(), degreeIteratorSetting);

            IteratorSetting fieldIteratorSetting = new IteratorSetting(7, SummingCombiner.class);
            SummingCombiner.setEncodingType(fieldIteratorSetting, LongCombiner.Type.STRING);
            SummingCombiner.setColumns(fieldIteratorSetting, Collections.singletonList(new IteratorSetting.Column("field", "")));
            tableOperations.attachIterator(getMetadataTable(), fieldIteratorSetting);

            Mutation mutation = new Mutation(PROPERTY);
            mutation.put(FIELD_DELIMITER_PROPERTY_NAME, EMPTY_CQ, defaultFieldDelimiter);
            mutation.put(FACT_DELIMITER_PROPERTY_NAME, EMPTY_CQ, defaultFactDelimiter);
            
            BatchWriterConfig bwConfig = new BatchWriterConfig();
            bwConfig.setMaxLatency(10000, TimeUnit.MINUTES);
            bwConfig.setMaxMemory(10000000);
            bwConfig.setMaxWriteThreads(5);
            bwConfig.setTimeout(5, TimeUnit.MINUTES);
            
            BatchWriter writer = connector.createBatchWriter(getMetadataTable(), bwConfig);
            writer.addMutation(mutation);
            writer.close();
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException e) {
            throw new D4MException("Error creating tables.", e);
        }

    }

    /* Pre-split the Tedge and TedgeText tables. Helpful when sha1 is used as row value. */
    public void addSplitsForSha1() {
        Validate.notNull(tableOperations, "tableOperations must not be null");

        String hexadecimal = "123456789abcde";
        SortedSet<Text> splits = new TreeSet<>();
        for (byte b : hexadecimal.getBytes(charset)) {
            splits.add(new Text(new byte[]{b}));
        }

        try {
            tableOperations.addSplits(getEdgeTable(), splits);
        } catch (TableNotFoundException e) {
            throw new D4MException(String.format("Unable to find table [%s]", getEdgeTable()), e);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new D4MException(String.format("Unable to add splits to table [%s]", getEdgeTable()), e);
        }
        
        try {
            tableOperations.addSplits(getTextTable(), splits);
        } catch (TableNotFoundException e) {
            throw new D4MException(String.format("Unable to find table [%s]", getEdgeTable()), e);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new D4MException(String.format("Unable to add splits to table [%s]", getEdgeTable()), e);
        }
    }

    public String getEdgeTable() {
        return "T" + getBaseTableName();
    }

    public String getTransposeTable() {
        return "T" + getBaseTableName() + "Transpose";
    }

    public String getDegreeTable() {
        return "T" + getBaseTableName() + "Degree";
    }

    public String getTextTable() {
        return "T" + getBaseTableName() + "Text";
    }

    public String getMetadataTable() {
        return "T" + getBaseTableName() + "Metadata";
    }

}
