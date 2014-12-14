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

/** Methods to create Accumulo tables for D4M schema.
 *
 * @author david medinets
 */
public class TableManager {

    @Setter
    @Getter
    private String rootName = "edge";
    
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

    /** Constructor
     *
     */
    public TableManager() {
    }

    /** Constructor with client-override of defaults.
     * 
     * @param connector Connector to Accumulo.
     */
    public TableManager(final Connector connector) {
        this.connector = connector;
        this.tableOperations = connector.tableOperations();
    }

    /** Constructor with client-override of defaults.
     * 
     * TableOperations can be passed in to allow mocks during testing.
     *
     * @param connector Connector to Accumulo.
     * @param tableOperations TableOperations object.
     */
    public TableManager(final Connector connector, final TableOperations tableOperations) {
        this.connector = connector;
        this.tableOperations = tableOperations;
    }

    /** Create D4M tables with a different root table name.
     * 
     * Five Accumulo tables support D4M. This method lets
     * you change the root of the table names.
     * 
     * T[root], T[root]Transpose, T[root]Degree, T[root]Metadata, T[root]Text
     * 
     * @param rootName string used to build D4M table names.
     */
    public void createTables(final String rootName) {
        setRootName(rootName);
        createTables();
    }

    /** Create D4M tables.
     *
     * Five Accumulo tables support D4M. This method creates them
     * using the default root (unless the caller changes that default).
     * 
     * Tedge, TedgeTranspose, TedgeDegree, TedgeMetadata, TedgeText
     */
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
            throw new D4MException("D4M: RootName[" + getRootName() + "] Inconsistent state - one or more D4M tables is missing.");
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

    /** Pre-split the Tedge and TedgeText tables. 
     * 
     * Helpful when sha1 is used as row value.
     */
    
    public void addSplitsForSha1() {
        Validate.notNull(tableOperations, "tableOperations must not be null");

        String hexadecimal = "123456789abcde";
        SortedSet<Text> splits = new TreeSet<>();
        for (byte b : hexadecimal.getBytes(charset)) {
            splits.add(new Text(new byte[]{b}));
        }

        addSplits(getEdgeTable(), splits);
        addSplits(getTextTable(), splits);
    }
    
    /** Pre-split the Tedge and TedgeText tables. 
     *
     * @param tablename name of the accumulo table
     * @param splits set of splits to add
     */
    public void addSplits(final String tablename, final SortedSet<Text> splits) {
        try {
            tableOperations.addSplits(tablename, splits);
        } catch (TableNotFoundException e) {
            throw new D4MException(String.format("Unable to find table [%s]", tablename), e);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new D4MException(String.format("Unable to add splits to table [%s]", tablename), e);
        }
    }

    /** Get the edge table name.
     *
     * @return the edge table name
     */
    public String getEdgeTable() {
        return "T" + getRootName();
    }

    /** Get the transpose table name.
     *
     * @return the transpose table name
     */
    public String getTransposeTable() {
        return "T" + getRootName() + "Transpose";
    }

    /** Get the degree table name.
     *
     * @return the degree table name
     */
    public String getDegreeTable() {
        return "T" + getRootName() + "Degree";
    }

    /** Get the text table name.
     *
     * @return the text table name
     */
    public String getTextTable() {
        return "T" + getRootName() + "Text";
    }

    /** Get the metadata table name.
     *
     * @return the metadata table name
     */
    public String getMetadataTable() {
        return "T" + getRootName() + "Metadata";
    }

}
