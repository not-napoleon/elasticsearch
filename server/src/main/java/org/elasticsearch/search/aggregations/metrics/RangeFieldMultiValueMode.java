package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.queries.BinaryDocValuesRangeQuery;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;

/**
 * Analog to {@link org.elasticsearch.search.MultiValueMode} providing multi value selection support to Range fields stored as binary doc
 * values.
 */
public enum RangeFieldMultiValueMode {

    MAX {
        @Override
        public double pick(SortedBinaryDocValues values, BinaryDocValuesRangeQuery.LengthType lengthType) throws IOException {
            BytesRef currentMax = null;
            for (int valueCounter = 0; valueCounter < values.docValueCount(); valueCounter++) {

                ByteArrayDataInput in = new ByteArrayDataInput();
                BytesRef otherFrom = new BytesRef();
                BytesRef otherTo = new BytesRef();
                BytesRef encodedRanges = values.nextValue();

                // the following is from BinaryDocValuesRangeQuery
                in.reset(encodedRanges.bytes, encodedRanges.offset, encodedRanges.length);
                int numRanges = in.readVInt();
                final byte[] bytes = encodedRanges.bytes;
                otherFrom.bytes = bytes;
                otherTo.bytes = bytes;
                int offset = in.getPosition();
                for (int i = 0; i < numRanges; i++) {
                    int length = lengthType.readLength(bytes, offset);
                    otherFrom.offset = offset;
                    otherFrom.length = length;
                    offset += length;

                    length = lengthType.readLength(bytes, offset);
                    otherTo.offset = offset;
                    otherTo.length = length;
                    offset += length;
                    // Max Specific logic
                    // TODO: currently hard coded to compare end points; need to inject a way to select the correct measure to compare
                    if (currentMax == null) {
                        currentMax = otherTo.clone();
                    } else {
                        if (currentMax.compareTo(otherTo) < 0) {
                            currentMax = otherTo.clone();
                        }
                    }
                }
            }
            // TODO: Hard coding to decoding a double here
            return NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(currentMax.bytes, 0));
        }
    };

    public abstract double pick(SortedBinaryDocValues values, BinaryDocValuesRangeQuery.LengthType lengthType) throws IOException;

    public NumericDoubleValues select(final SortedBinaryDocValues values, BinaryDocValuesRangeQuery.LengthType lengthType) {
        return new NumericDoubleValues() {

            private double value;

            @Override
            public boolean advanceExact(int target) throws IOException {
                if (values.advanceExact(target)) {
                    value = pick(values, lengthType);
                    return true;
                }
                return false;
            }

            @Override
            public double doubleValue() throws IOException {
                return this.value;
            }
        };
    }
}
