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
}

// End FastBatchingCellReaderTest.java

