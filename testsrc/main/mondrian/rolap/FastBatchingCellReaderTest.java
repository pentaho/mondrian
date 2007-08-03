/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.agg.SegmentLoader;
import mondrian.rolap.agg.GroupingSet;
import mondrian.test.TestContext;
import mondrian.test.SqlPattern;

import java.util.*;

/**
 * Test for <code>FastBatchingCellReader</code>.
 *
 * @author Thiyagu
 * @version $Id$
 * @since 24-May-2007
 */
public class FastBatchingCellReaderTest extends BatchTestCase {

    public void testShouldUseGroupingFunctionOnPropertyTrueAndOnSupportedDB() {
        boolean oldValue = MondrianProperties.instance().EnableGroupingSets.get();
        MondrianProperties.instance().EnableGroupingSets.set(true);
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null) {
            boolean doesDBSupportGroupingSets() {
                return true;
            }
        };
        assertTrue(fbcr.shouldUseGroupingFunction());
        MondrianProperties.instance().EnableGroupingSets.set(oldValue);
    }

    public void testShouldUseGroupingFunctionOnPropertyTrueAndOnNonSupportedDB() {
        boolean oldValue = MondrianProperties.instance().EnableGroupingSets.get();
        MondrianProperties.instance().EnableGroupingSets.set(true);
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null) {
            boolean doesDBSupportGroupingSets() {
                return false;
            }
        };
        assertFalse(fbcr.shouldUseGroupingFunction());
        MondrianProperties.instance().EnableGroupingSets.set(oldValue);
    }

    public void testShouldUseGroupingFunctionOnPropertyFalseOnSupportedDB() {
        boolean oldValue = MondrianProperties.instance().EnableGroupingSets.get();
        MondrianProperties.instance().EnableGroupingSets.set(false);
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null) {
            boolean doesDBSupportGroupingSets() {
                return true;
            }
        };
        assertFalse(fbcr.shouldUseGroupingFunction());
        MondrianProperties.instance().EnableGroupingSets.set(oldValue);
    }

    public void testShouldUseGroupingFunctionOnPropertyFalseOnNonSupportedDB() {
        boolean oldValue = MondrianProperties.instance().EnableGroupingSets.get();
        MondrianProperties.instance().EnableGroupingSets.set(false);
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null) {
            boolean doesDBSupportGroupingSets() {
                return false;
            }
        };
        assertFalse(fbcr.shouldUseGroupingFunction());
        MondrianProperties.instance().EnableGroupingSets.set(oldValue);
    }

    public void testDoesDBSupportGroupingSets() {
        final SqlQuery.Dialect dialect = getTestContext().getDialect();
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null) {
            SqlQuery.Dialect getDialect() {
                return dialect;
            }
        };
        if (dialect.isOracle() || dialect.isTeradata() || dialect.isDB2()) {
            assertTrue(fbcr.doesDBSupportGroupingSets());
        } else {
            assertFalse(fbcr.doesDBSupportGroupingSets());
        }
    }

    public void testGroupBatchesForNonGroupableBatchesWithSorting() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);
        FastBatchingCellReader.Batch genderBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F"));
        FastBatchingCellReader.Batch maritalStatusBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "marital_status", "M"));
        ArrayList<FastBatchingCellReader.Batch> batchList =
            new ArrayList<FastBatchingCellReader.Batch>();
        batchList.add(genderBatch);
        batchList.add(maritalStatusBatch);
        List<FastBatchingCellReader.CompositeBatch> groupedBatches =
            fbcr.groupBatches(batchList);
        assertEquals(batchList.size(), groupedBatches.size());
        assertEquals(genderBatch, groupedBatches.get(0).detailedBatch);
        assertEquals(maritalStatusBatch, groupedBatches.get(1).detailedBatch);
    }

    public void testGroupBatchesForGroupableBatches() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);
        FastBatchingCellReader.Batch genderBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F")) {
            boolean canBatch(FastBatchingCellReader.Batch other) {
                return false;
            }
        };
        FastBatchingCellReader.Batch superBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                new String[0], new String[0], new String[0])) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                return true;
            }
        };
        ArrayList<FastBatchingCellReader.Batch> batchList =
            new ArrayList<FastBatchingCellReader.Batch>();
        batchList.add(genderBatch);
        batchList.add(superBatch);
        List<FastBatchingCellReader.CompositeBatch> groupedBatches =
            fbcr.groupBatches(batchList);
        assertEquals(1, groupedBatches.size());
        assertEquals(superBatch, groupedBatches.get(0).detailedBatch);
        assertTrue(
            groupedBatches.get(0).summaryBatches.contains(genderBatch));
    }

    public void testGroupBatchesForGroupableBatchesAndNonGroupableBatches() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);
        final FastBatchingCellReader.Batch group1Agg2 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F")) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                return false;
            }
        };
        final FastBatchingCellReader.Batch group1Agg1 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F")) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                if (batch.equals(group1Agg2)) {
                    return true;
                }
                return false;
            }
        };
        FastBatchingCellReader.Batch group1Detailed = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                new String[0], new String[0], new String[0])) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                if (batch.equals(group1Agg1)) {
                    return true;
                } else {
                    return false;
                }
            }
        };
        final FastBatchingCellReader.Batch group2Agg1 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "education", "F")) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                return false;
            }
        };
        FastBatchingCellReader.Batch group2Detailed = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "yearly_income", "")) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                if (batch.equals(group2Agg1)) {
                    return true;
                } else {
                    return false;
                }
            }
        };
        ArrayList<FastBatchingCellReader.Batch> batchList =
            new ArrayList<FastBatchingCellReader.Batch>();
        batchList.add(group1Agg1);
        batchList.add(group1Agg2);
        batchList.add(group1Detailed);
        batchList.add(group2Agg1);
        batchList.add(group2Detailed);
        List<FastBatchingCellReader.CompositeBatch> groupedBatches =
            fbcr.groupBatches(batchList);
        assertEquals(2, groupedBatches.size());
        assertEquals(group1Detailed, groupedBatches.get(0).detailedBatch);
        assertTrue(groupedBatches.get(0).summaryBatches.contains(group1Agg1));
        assertTrue(groupedBatches.get(0).summaryBatches.contains(group1Agg2));
        assertEquals(group2Detailed, groupedBatches.get(1).detailedBatch);
        assertTrue(groupedBatches.get(1).summaryBatches.contains(group2Agg1));
    }

    public void testAddToCompositeBatchForBothBatchesNotPartOfCompositeBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);
        FastBatchingCellReader.Batch batch1 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        FastBatchingCellReader.Batch batch2 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F"));
        Map<FastBatchingCellReader.BatchKey, FastBatchingCellReader.CompositeBatch> batchGroups =
            new HashMap<FastBatchingCellReader.BatchKey, FastBatchingCellReader.CompositeBatch>();
        fbcr.addToCompositeBatch(batchGroups, batch1, batch2);
        assertEquals(1, batchGroups.size());
        FastBatchingCellReader.CompositeBatch compositeBatch =
            batchGroups.get(batch1.batchKey);
        assertEquals(batch1, compositeBatch.detailedBatch);
        assertEquals(1, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(batch2));
    }

    public void testAddToCompositeBatchForDetailedBatchAlreadyPartOfACompositeBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);
        FastBatchingCellReader.Batch detailedBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        FastBatchingCellReader.Batch aggBatch1 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F"));
        FastBatchingCellReader.Batch aggBatchAlreadyInCompisite =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "gender", "F"));
        Map<FastBatchingCellReader.BatchKey, FastBatchingCellReader.CompositeBatch> batchGroups =
            new HashMap<FastBatchingCellReader.BatchKey, FastBatchingCellReader.CompositeBatch>();
        FastBatchingCellReader.CompositeBatch existingCompositeBatch =
            fbcr.new CompositeBatch(detailedBatch);
        existingCompositeBatch.add(aggBatchAlreadyInCompisite);
        batchGroups.put(detailedBatch.batchKey, existingCompositeBatch);

        fbcr.addToCompositeBatch(batchGroups, detailedBatch, aggBatch1);

        assertEquals(1, batchGroups.size());
        FastBatchingCellReader.CompositeBatch compositeBatch =
            batchGroups.get(detailedBatch.batchKey);
        assertEquals(detailedBatch, compositeBatch.detailedBatch);
        assertEquals(2, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(aggBatch1));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInCompisite));
    }

    public void testAddToCompositeBatchForAggregationBatchAlreadyPartOfACompositeBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);
        FastBatchingCellReader.Batch detailedBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        FastBatchingCellReader.Batch aggBatchToAddToDetailedBatch =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "gender", "F"));
        FastBatchingCellReader.Batch aggBatchAlreadyInCompisite =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "city", "F"));
        Map<FastBatchingCellReader.BatchKey, FastBatchingCellReader.CompositeBatch> batchGroups =
            new HashMap<FastBatchingCellReader.BatchKey, FastBatchingCellReader.CompositeBatch>();
        FastBatchingCellReader.CompositeBatch existingCompositeBatch =
            fbcr.new CompositeBatch(aggBatchToAddToDetailedBatch);
        existingCompositeBatch.add(aggBatchAlreadyInCompisite);
        batchGroups.put(aggBatchToAddToDetailedBatch.batchKey,
            existingCompositeBatch);

        fbcr.addToCompositeBatch(batchGroups, detailedBatch,
            aggBatchToAddToDetailedBatch);

        assertEquals(1, batchGroups.size());
        FastBatchingCellReader.CompositeBatch compositeBatch =
            batchGroups.get(detailedBatch.batchKey);
        assertEquals(detailedBatch, compositeBatch.detailedBatch);
        assertEquals(2, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchToAddToDetailedBatch));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInCompisite));
    }

    public void testAddToCompositeBatchForBothBatchAlreadyPartOfACompositeBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);
        FastBatchingCellReader.Batch detailedBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        FastBatchingCellReader.Batch aggBatchToAddToDetailedBatch =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "gender", "F"));
        FastBatchingCellReader.Batch aggBatchAlreadyInCompositeOfAgg =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "city", "F"));
        FastBatchingCellReader.Batch aggBatchAlreadyInCompositeOfDetail =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "state_province",
                "F"));

        Map<FastBatchingCellReader.BatchKey, FastBatchingCellReader.CompositeBatch> batchGroups =
            new HashMap<FastBatchingCellReader.BatchKey, FastBatchingCellReader.CompositeBatch>();
        FastBatchingCellReader.CompositeBatch existingAggCompositeBatch =
            fbcr.new CompositeBatch(aggBatchToAddToDetailedBatch);
        existingAggCompositeBatch.add(aggBatchAlreadyInCompositeOfAgg);
        batchGroups.put(aggBatchToAddToDetailedBatch.batchKey,
            existingAggCompositeBatch);

        FastBatchingCellReader.CompositeBatch existingCompositeBatch =
            fbcr.new CompositeBatch(detailedBatch);
        existingCompositeBatch.add(aggBatchAlreadyInCompositeOfDetail);
        batchGroups.put(detailedBatch.batchKey, existingCompositeBatch);

        fbcr.addToCompositeBatch(batchGroups, detailedBatch,
            aggBatchToAddToDetailedBatch);

        assertEquals(1, batchGroups.size());
        FastBatchingCellReader.CompositeBatch compositeBatch =
            batchGroups.get(detailedBatch.batchKey);
        assertEquals(detailedBatch, compositeBatch.detailedBatch);
        assertEquals(3, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchToAddToDetailedBatch));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInCompositeOfAgg));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInCompositeOfDetail));
    }

    public void testCanBatchForBatchWithSuperSetOfContraintColumnBitKeyAndAllValuesForAdditionalCondition() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        assertTrue(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithDistinctCountInDetailedBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment},
                cubeNameSales, measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment},
                cubeNameSales,
                "[Measures].[Customer Count]");

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithDistinctCountInAggregateBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                "[Measures].[Customer Count]");

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment},
                cubeNameSales, measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithNonSuperSetOfContraintColumnBitKeyAndAllValuesForAdditionalCondition() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithSuperSetOfContraintColumnBitKeyAndNOTAllValuesForAdditionalCondition() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, new String[]{"M"}},
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchesFromSameAggregationButDifferentRollupOption() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);

        FastBatchingCellReader.Batch batch1 =
                createBatch(fbcr,
                        new String[]{tableTime}, new String[]{fieldYear},
                        new String[][]{fieldValuesYear}, cubeNameSales,
                        measureUnitSales);

        FastBatchingCellReader.Batch batch2 =
                createBatch(fbcr,
                        new String[]{tableTime, tableTime, tableTime},
                        new String[]{fieldYear, "quarter", "month_of_year"},
                        new String[][]{fieldValuesYear, {"Q1", "Q2", "Q3",
                                "Q4"},
                                {"1", "2", "3", "4", "5", "6", "7", "8",
                                        "9", "10", "11", "12"}},
                        cubeNameSales,
                        measureUnitSales);

        if (MondrianProperties.instance().UseAggregates.get()
                && MondrianProperties.instance().ReadAggregates.get()) {

            assertFalse(batch2.canBatch(batch1));
            assertFalse(batch1.canBatch(batch2));
        } else {
            if (!Util.PreJdk15) {
                // In JDK1.4, Trigger which controls UseAggregates is not working
                // properly, which causes this test to fail
                assertTrue(batch2.canBatch(batch1));
            }
        }
    }

    public void testCanBatchForBatchWithSuperSetOfContraintColumnBitKeyAndDifferentValuesForOverlappingColumns() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{new String[]{"1997"},
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{new String[]{"1998"},
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithDifferentAggregationTable() {
        getTestContext().clearConnection();
        boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get();
        boolean readAggregates =
            MondrianProperties.instance().ReadAggregates.get();
        MondrianProperties.instance().UseAggregates.set(true);
        MondrianProperties.instance().ReadAggregates.set(true);

        FastBatchingCellReader fbcr =
            new FastBatchingCellReader(getCube(cubeNameSales));

        FastBatchingCellReader.Batch summaryBatch =
            createBatch(fbcr,
                new String[]{tableTime}, new String[]{fieldYear},
                new String[][]{fieldValuesYear}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableCustomer},
                new String[]{fieldYear, fieldGender},
                new String[][]{fieldValuesYear, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(summaryBatch));
        assertFalse(summaryBatch.canBatch(detailedBatch));
        MondrianProperties.instance().UseAggregates.set(useAggregates);
        MondrianProperties.instance().ReadAggregates.set(readAggregates);
    }


    public void testCompositeBatchLoadAggregation() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(null);

        FastBatchingCellReader.Batch summaryBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        final List<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        FastBatchingCellReader.CompositeBatch compositeBatch =
            fbcr.new CompositeBatch(detailedBatch) {
                SegmentLoader getSegmentLoader() {
                    return new SegmentLoader() {
                        public void load(
                            List<GroupingSet> sets,
                            RolapAggregationManager.PinSet pinnedSegments)
                        {
                            groupingSets.addAll(sets);
                        }
                    };
                }
            };
        compositeBatch.add(summaryBatch);

        compositeBatch.loadAggregation();

        assertEquals(2, groupingSets.size());
        GroupingSetsCollector detailedCollector =
            new GroupingSetsCollector(true);
        detailedBatch.loadAggregation(detailedCollector);
        List<GroupingSet> baseGroupingSet =
            detailedCollector.getGroupingSets();
        GroupingSetsCollector summaryCollector =
            new GroupingSetsCollector(true);
        summaryBatch.loadAggregation(summaryCollector);
        List<GroupingSet> groupingSet =
            summaryCollector.getGroupingSets();

        assertEquals(baseGroupingSet.get(0).getLevelBitKey(),
            groupingSets.get(0).getLevelBitKey());
        assertEquals(groupingSet.get(0).getLevelBitKey(),
            groupingSets.get(1).getLevelBitKey());

    }

    /**
     * Checks that in dialects that request it (e.g. LucidDB),
     * distinct aggregates based on SQL expressions,
     * e.g. <code>count(distinct "col1" + "col2"), count(distinct query)</code>,
     * are loaded individually, and separately from the other aggregates.
     */
    public void testLoadDistinctSqlMeasure() {
        // Some databases cannot handle scalar subqueries inside
        // count(distinct).
        final SqlQuery.Dialect dialect = getTestContext().getDialect();
        switch (SqlPattern.Dialect.get(dialect)) {
        case ORACLE: // gives 'feature not supported' in Express 10.2
            return;
        }

        String cube =
            "<Cube name=\"Warehouse2\">" +
            "   <Table name=\"warehouse\"/>" +
            "   <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"stores_id\"/>" +
            "   <Measure name=\"Count Distinct of Warehouses (Large Owned)\" aggregator=\"distinct count\" formatString=\"#,##0\">" +
            "       <MeasureExpression>" +
            "       <SQL dialect=\"generic\">(select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Owned')</SQL>" +
            "       </MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Count Distinct of Warehouses (Large Independent)\" aggregator=\"distinct count\" formatString=\"#,##0\">" +
            "       <MeasureExpression>" +
            "       <SQL dialect=\"generic\">(select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')</SQL>" +
            "       </MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Count All of Warehouses (Large Independent)\" aggregator=\"count\" formatString=\"#,##0\">" +
            "       <MeasureExpression>" +
            "           <SQL dialect=\"generic\">(select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')</SQL>" +
            "       </MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Count Distinct Store+Warehouse\" aggregator=\"distinct count\" formatString=\"#,##0\">" +
            "       <MeasureExpression><SQL dialect=\"generic\">`store_id`+`warehouse_id`</SQL></MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Count All Store+Warehouse\" aggregator=\"count\" formatString=\"#,##0\">" +
            "       <MeasureExpression><SQL dialect=\"generic\">`store_id`+`warehouse_id`</SQL></MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Store Count\" column=\"stores_id\" aggregator=\"count\" formatString=\"#,###\"/>" +
            "</Cube>";
        cube = cube.replaceAll("`", dialect.getQuoteIdentifierString());
        if (dialect.isOracle()) {
            cube = cube.replaceAll(" AS ", " ");
        }

        String query =
            "select " +
            "   [Store Type].Children on rows, " +
            "   {[Measures].[Count Distinct of Warehouses (Large Owned)]," +
            "    [Measures].[Count Distinct of Warehouses (Large Independent)]," +
            "    [Measures].[Count All of Warehouses (Large Independent)]," +
            "    [Measures].[Count Distinct Store+Warehouse]," +
            "    [Measures].[Count All Store+Warehouse]," +
            "    [Measures].[Store Count]} on columns " +
            "from [Warehouse2]";

        TestContext testContext =
            TestContext.create(
             null,
             cube,
             null,
             null,
             null);

        testContext.assertQueryReturns(
            query,
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Count Distinct of Warehouses (Large Owned)]}\n" +
                "{[Measures].[Count Distinct of Warehouses (Large Independent)]}\n" +
                "{[Measures].[Count All of Warehouses (Large Independent)]}\n" +
                "{[Measures].[Count Distinct Store+Warehouse]}\n" +
                "{[Measures].[Count All Store+Warehouse]}\n" +
                "{[Measures].[Store Count]}\n" +
                "Axis #2:\n" +
                "{[Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
                "{[Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
                "{[Store Type].[All Store Types].[HeadQuarters]}\n" +
                "{[Store Type].[All Store Types].[Mid-Size Grocery]}\n" +
                "{[Store Type].[All Store Types].[Small Grocery]}\n" +
                "{[Store Type].[All Store Types].[Supermarket]}\n" +
                "Row #0: 1\n" +
                "Row #0: 0\n" +
                "Row #0: 0\n" +
                "Row #0: 6\n" +
                "Row #0: 6\n" +
                "Row #0: 6\n" +
                "Row #1: 1\n" +
                "Row #1: 0\n" +
                "Row #1: 0\n" +
                "Row #1: 2\n" +
                "Row #1: 2\n" +
                "Row #1: 2\n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #3: 0\n" +
                "Row #3: 1\n" +
                "Row #3: 1\n" +
                "Row #3: 4\n" +
                "Row #3: 4\n" +
                "Row #3: 4\n" +
                "Row #4: 0\n" +
                "Row #4: 1\n" +
                "Row #4: 1\n" +
                "Row #4: 4\n" +
                "Row #4: 4\n" +
                "Row #4: 4\n" +
                "Row #5: 0\n" +
                "Row #5: 1\n" +
                "Row #5: 3\n" +
                "Row #5: 8\n" +
                "Row #5: 8\n" +
                "Row #5: 8\n"));

        String loadCountDistinct_luciddb1 =
            "select " +
            "\"store\".\"store_type\" as \"c0\", " +
            "count(distinct " +
            "(select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" " +
            "from \"warehouse_class\" AS \"warehouse_class\" " +
            "where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Owned')) as \"m0\" " +
            "from \"store\" as \"store\", \"warehouse\" as \"warehouse\" " +
            "where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" " +
            "group by \"store\".\"store_type\"";

        String loadCountDistinct_luciddb2 =
            "select " +
            "\"store\".\"store_type\" as \"c0\", " +
            "count(distinct " +
            "(select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" " +
            "from \"warehouse_class\" AS \"warehouse_class\" " +
            "where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\" " +
            "from \"store\" as \"store\", \"warehouse\" as \"warehouse\" " +
            "where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" " +
            "group by \"store\".\"store_type\"";

        String loadOtherAggs_luciddb =
            "select " +
            "\"store\".\"store_type\" as \"c0\", " +
            "count(" +
            "(select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" " +
            "from \"warehouse_class\" AS \"warehouse_class\" " +
            "where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\", " +
            "count(distinct \"store_id\"+\"warehouse_id\") as \"m1\", " +
            "count(\"store_id\"+\"warehouse_id\") as \"m2\", " +
            "count(\"warehouse\".\"stores_id\") as \"m3\" " +
            "from \"store\" as \"store\", \"warehouse\" as \"warehouse\" " +
            "where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" " +
            "group by \"store\".\"store_type\"";

        // Derby splits into multiple statements.
        String loadCountDistinct_derby1 = "select \"store\".\"store_type\" as \"c0\", count(distinct (select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" from \"warehouse_class\" AS \"warehouse_class\" where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Owned')) as \"m0\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";
            String loadCountDistinct_derby2 = "select \"store\".\"store_type\" as \"c0\", count(distinct (select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" from \"warehouse_class\" AS \"warehouse_class\" where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";
            String loadCountDistinct_derby3 = "select \"store\".\"store_type\" as \"c0\", count(distinct \"store_id\"+\"warehouse_id\") as \"m0\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";
            String loadOtherAggs_derby = "select \"store\".\"store_type\" as \"c0\", count((select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" from \"warehouse_class\" AS \"warehouse_class\" where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\", count(\"store_id\"+\"warehouse_id\") as \"m1\", count(\"warehouse\".\"stores_id\") as \"m2\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";

        // MySQL does it in one statement.
        String load_mysql = "select"
            + " `store`.`store_type` as `c0`,"
            + " count(distinct (select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Owned')) as `m0`,"
            + " count(distinct (select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')) as `m1`,"
            + " count((select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')) as `m2`,"
            + " count(distinct `store_id`+`warehouse_id`) as `m3`,"
            + " count(`store_id`+`warehouse_id`) as `m4`,"
            + " count(`warehouse`.`stores_id`) as `m5` "
            + "from `store` as `store`,"
            + " `warehouse` as `warehouse` "
            + "where `warehouse`.`stores_id` = `store`.`store_id` "
            + "group by `store`.`store_type`";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.LUCIDDB, loadCountDistinct_luciddb1, loadCountDistinct_luciddb1),
            new SqlPattern(SqlPattern.Dialect.LUCIDDB, loadCountDistinct_luciddb2, loadCountDistinct_luciddb2),
            new SqlPattern(SqlPattern.Dialect.LUCIDDB, loadOtherAggs_luciddb, loadOtherAggs_luciddb),

            new SqlPattern(SqlPattern.Dialect.DERBY, loadCountDistinct_derby1, loadCountDistinct_derby1),
            new SqlPattern(SqlPattern.Dialect.DERBY, loadCountDistinct_derby2, loadCountDistinct_derby2),
            new SqlPattern(SqlPattern.Dialect.DERBY, loadCountDistinct_derby3, loadCountDistinct_derby3),
            new SqlPattern(SqlPattern.Dialect.DERBY, loadOtherAggs_derby, loadOtherAggs_derby),

            new SqlPattern(SqlPattern.Dialect.MYSQL, load_mysql, load_mysql),
        };

        assertQuerySql(testContext, query, patterns);
    }
}

// End FastBatchingCellReaderTest.java

