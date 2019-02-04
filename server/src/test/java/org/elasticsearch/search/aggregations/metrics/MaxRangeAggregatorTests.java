/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.io.IOException;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class MaxRangeAggregatorTests extends AggregatorTestCase {

    private static final String EXPECTED_FIELD_NAME = "range";

    public void testSimpleRangeMax() throws IOException {
        RangeFieldMapper.RangeType rangeType = RangeFieldMapper.RangeType.LONG;
        BytesRef encodedRange =
            rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, 1.0D, 10.0D, true , true)));

        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new BinaryDocValuesField(EXPECTED_FIELD_NAME, encodedRange)));
        }, max -> {
            assertEquals(10.0D, max.getValue(), 0);
        });
    }

    // TODO: Tests for all the various range query modes

    private void testCase(Query query,
                            CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                            Consumer<InternalMax> verify) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
        MaxRangeAggregationBuilder aggregationBuilder = new MaxRangeAggregationBuilder(EXPECTED_FIELD_NAME).field(EXPECTED_FIELD_NAME);

        MappedFieldType fieldType = new RangeFieldMapper.Builder(EXPECTED_FIELD_NAME, RangeFieldMapper.RangeType.DOUBLE).fieldType();
        fieldType.setName(EXPECTED_FIELD_NAME);

        MaxRangeAggregator aggregator = createAggregator(query, aggregationBuilder, indexSearcher, createIndexSettings(), fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalMax) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }
}
