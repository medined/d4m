package com.codebits.d4m;

import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.io.Text;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

/** Tests for the TableManager class
 *
 * @author david
 */
public class TableManagerTest {

    Connector mockConnector = mock(Connector.class);
    TableOperations mockTableOperations = mock(TableOperations.class);

    private TableManager instance = null;

    /** Setup instance for each test.
     */
    @Before
    public void setup() {
        instance = new TableManager();
        instance.setConnector(mockConnector);
        instance.setTableOperations(mockTableOperations);
    }

    /** Test constructor with Connector and Table operations
     */
    @Test
    public void testConstructorWithConnectorAndTableOperations() {
        instance = new TableManager(mockConnector, mockTableOperations);
        assertEquals(mockConnector, instance.getConnector());
        assertEquals(mockTableOperations, instance.getTableOperations());
    }
    
    /** Test CreateTables with Root Name
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test
    public void testCreateTablesWithRootName() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.exists("TTEST")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TTESTTranspose")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TTESTDegree")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TTESTMetadata")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TTESTText")).thenReturn(Boolean.FALSE);

        BatchWriterConfig bwConfig = new BatchWriterConfig();
        bwConfig.setMaxLatency(10000, TimeUnit.MINUTES);
        bwConfig.setMaxMemory(10000000);
        bwConfig.setMaxWriteThreads(5);
        bwConfig.setTimeout(5, TimeUnit.MINUTES);

        BatchWriter mockBatchWriter = mock(BatchWriter.class);
        when(mockConnector.createBatchWriter("TTESTMetadata", bwConfig)).thenReturn(mockBatchWriter);
        instance.createTables("TEST");
        verify(mockTableOperations, times(5)).exists(any(String.class));
        verify(mockTableOperations).create("TTEST");
        verify(mockTableOperations).create("TTESTTranspose");
        verify(mockTableOperations).create("TTESTDegree");
        verify(mockTableOperations).create("TTESTMetadata");
        verify(mockTableOperations).create("TTESTText");
        verify(mockTableOperations).attachIterator(matches("TTESTDegree"), any(IteratorSetting.class));
        verify(mockTableOperations).attachIterator(matches("TTESTMetadata"), any(IteratorSetting.class));
        verifyNoMoreInteractions(mockTableOperations);

        verify(mockBatchWriter).addMutation(any(Mutation.class));
        verify(mockBatchWriter).close();
        verifyNoMoreInteractions(mockBatchWriter);
    }
    
    /** Test CreateTables
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test
    public void testCreateTables() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.exists("Tedge")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeTranspose")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeDegree")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeMetadata")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeText")).thenReturn(Boolean.FALSE);

        BatchWriterConfig bwConfig = new BatchWriterConfig();
        bwConfig.setMaxLatency(10000, TimeUnit.MINUTES);
        bwConfig.setMaxMemory(10000000);
        bwConfig.setMaxWriteThreads(5);
        bwConfig.setTimeout(5, TimeUnit.MINUTES);

        BatchWriter mockBatchWriter = mock(BatchWriter.class);
        when(mockConnector.createBatchWriter("TedgeMetadata", bwConfig)).thenReturn(mockBatchWriter);

        instance.createTables();
        verify(mockTableOperations, times(5)).exists(any(String.class));
        verify(mockTableOperations).create("Tedge");
        verify(mockTableOperations).create("TedgeTranspose");
        verify(mockTableOperations).create("TedgeDegree");
        verify(mockTableOperations).create("TedgeMetadata");
        verify(mockTableOperations).create("TedgeText");
        verify(mockTableOperations).attachIterator(matches("TedgeDegree"), any(IteratorSetting.class));
        verify(mockTableOperations).attachIterator(matches("TedgeMetadata"), any(IteratorSetting.class));
        verifyNoMoreInteractions(mockTableOperations);

        verify(mockBatchWriter).addMutation(any(Mutation.class));
        verify(mockBatchWriter).close();
        verifyNoMoreInteractions(mockBatchWriter);
    }
    
    /** Test CreateTable can handle AccumuloException
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test(expected = AccumuloException.class)
    public void testCreateTableHandlesAccumuloException() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.exists("Tedge")).thenThrow(AccumuloException.class);
        instance.createTables();
    }

    /** Test AddSplitsForSha1
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test
    public void testAddSplitsForSha1() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.listSplits("Tedge")).thenReturn(new ArrayList<Text>());
        when(mockTableOperations.listSplits("TedgeText")).thenReturn(new ArrayList<Text>());
        instance.addSplitsForSha1();
        verify(mockTableOperations).listSplits(matches("Tedge"));
        verify(mockTableOperations).listSplits(matches("TedgeText"));
        verify(mockTableOperations).addSplits(matches("Tedge"), any(SortedSet.class));
        verify(mockTableOperations).addSplits(matches("TedgeText"), any(SortedSet.class));
        verifyNoMoreInteractions(mockTableOperations);
    }

    /** Test CreateTables with all existing does nothing
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test
    public void testCreateTables_with_all_existing_does_nothing() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.exists("Tedge")).thenReturn(Boolean.TRUE);
        when(mockTableOperations.exists("TedgeTranspose")).thenReturn(Boolean.TRUE);
        when(mockTableOperations.exists("TedgeDegree")).thenReturn(Boolean.TRUE);
        when(mockTableOperations.exists("TedgeMetadata")).thenReturn(Boolean.TRUE);
        when(mockTableOperations.exists("TedgeText")).thenReturn(Boolean.TRUE);
        instance.createTables();
        verify(mockTableOperations).exists("Tedge");
        verify(mockTableOperations).exists("TedgeTranspose");
        verify(mockTableOperations).exists("TedgeDegree");
        verify(mockTableOperations).exists("TedgeMetadata");
        verify(mockTableOperations).exists("TedgeText");
        verifyNoMoreInteractions(mockTableOperations);
    }

    /** Test CreateTables with just edge table
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test(expected = D4MException.class)
    public void testCreateTables_with_just_edge_table() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.exists("Tedge")).thenReturn(Boolean.TRUE);
        when(mockTableOperations.exists("TedgeTranspose")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeDegree")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeMetadata")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeText")).thenReturn(Boolean.FALSE);
        instance.createTables();
    }

    /** Test CreateTables with just transpose table
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test(expected = D4MException.class)
    public void testCreateTables_with_just_transpose_table() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.exists("Tedge")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeTranspose")).thenReturn(Boolean.TRUE);
        when(mockTableOperations.exists("TedgeDegree")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeMetadata")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeText")).thenReturn(Boolean.FALSE);
        instance.createTables();
    }

    /** Test CreateTables with just degree table
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test(expected = D4MException.class)
    public void testCreateTables_with_just_degree_table() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.exists("Tedge")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeTranspose")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeDegree")).thenReturn(Boolean.TRUE);
        when(mockTableOperations.exists("TedgeMetadata")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeText")).thenReturn(Boolean.FALSE);
        instance.createTables();
    }

    /** Test CreateTables with just field table
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test(expected = D4MException.class)
    public void testCreateTables_with_just_field_table() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.exists("Tedge")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeTranspose")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeDegree")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeMetadata")).thenReturn(Boolean.TRUE);
        when(mockTableOperations.exists("TedgeText")).thenReturn(Boolean.FALSE);
        instance.createTables();
    }

    /** Test CreateTables with just text table
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test(expected = D4MException.class)
    public void testCreateTables_with_just_text_table() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        when(mockTableOperations.exists("Tedge")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeTranspose")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeDegree")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeMetadata")).thenReturn(Boolean.FALSE);
        when(mockTableOperations.exists("TedgeText")).thenReturn(Boolean.TRUE);
        instance.createTables();
    }

    /** Test CreateTables with null tableOperation
     *
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateTables_with_null_tableOperation() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        instance.setTableOperations(null);
        instance.createTables();
    }

}
