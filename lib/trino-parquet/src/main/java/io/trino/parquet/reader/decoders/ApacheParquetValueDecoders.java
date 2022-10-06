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
package io.trino.parquet.reader.decoders;

import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetReaderUtils.castToByte;
import static io.trino.parquet.ParquetReaderUtils.toByteExact;
import static io.trino.parquet.ParquetReaderUtils.toShortExact;
import static io.trino.parquet.ParquetTypeUtils.checkBytesFitInShortDecimal;
import static io.trino.parquet.ParquetTypeUtils.getShortDecimalValue;
import static java.util.Objects.requireNonNull;

/**
 * This is a set of proxy value decoders that use a delegated value reader from apache lib.
 */
public class ApacheParquetValueDecoders
{
    private ApacheParquetValueDecoders() {}

    public static final class IntApacheParquetValueDecoder
            implements ValueDecoder<int[]>
    {
        private final ValuesReader delegate;

        public IntApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(int[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = delegate.readInteger();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class ShortApacheParquetValueDecoder
            implements ValueDecoder<short[]>
    {
        private final ValuesReader delegate;

        public ShortApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(short[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = toShortExact(delegate.readInteger());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class ByteApacheParquetValueDecoder
            implements ValueDecoder<byte[]>
    {
        private final ValuesReader delegate;

        public ByteApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(byte[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = toByteExact(delegate.readInteger());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class IntToLongApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;

        public IntToLongApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = delegate.readInteger();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class LongApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;

        public LongApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = delegate.readLong();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class DoubleApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;

        public DoubleApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = Double.doubleToLongBits(delegate.readDouble());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class FloatApacheParquetValueDecoder
            implements ValueDecoder<int[]>
    {
        private final ValuesReader delegate;

        public FloatApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(int[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = Float.floatToIntBits(delegate.readFloat());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class BooleanApacheParquetValueDecoder
            implements ValueDecoder<byte[]>
    {
        private final ValuesReader delegate;

        public BooleanApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            byte[] buffer = input.readBytes();
            try {
                // Deprecated PLAIN boolean decoder from Apache lib is the only one that actually
                // uses the valueCount argument to allocate memory so we simulate it here.
                int valueCount = buffer.length * Byte.SIZE;
                delegate.initFromPage(valueCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, 0, buffer.length)));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void read(byte[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = castToByte(delegate.readBoolean());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class ShortDecimalApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;
        private final DecimalType decimalType;
        private final ColumnDescriptor descriptor;
        private final int typeLength;

        public ShortDecimalApacheParquetValueDecoder(ValuesReader delegate, DecimalType decimalType, ColumnDescriptor descriptor)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            checkArgument(decimalType.isShort(), "Decimal type %s is not a short decimal", decimalType);
            this.decimalType = decimalType;
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.typeLength = descriptor.getPrimitiveType().getTypeLength();
            checkArgument(typeLength > 0 && typeLength <= 16, "Expected column %s to have type length in range (1-16)", descriptor);
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            int bytesOffset = 0;
            int bytesLength = typeLength;
            if (typeLength > Long.BYTES) {
                bytesOffset = typeLength - Long.BYTES;
                bytesLength = Long.BYTES;
            }
            for (int i = offset; i < offset + length; i++) {
                byte[] bytes = delegate.readBytes().getBytes();
                checkBytesFitInShortDecimal(bytes, 0, bytesOffset, decimalType, descriptor);
                values[i] = getShortDecimalValue(bytes, bytesOffset, bytesLength);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class LongDecimalApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;

        public LongDecimalApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            int endOffset = (offset + length) * 2;
            for (int currentOutputOffset = offset * 2; currentOutputOffset < endOffset; currentOutputOffset += 2) {
                Int128 value = Int128.fromBigEndian(delegate.readBytes().getBytes());
                values[currentOutputOffset] = value.getHigh();
                values[currentOutputOffset + 1] = value.getLow();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static void initialize(SimpleSliceInputStream input, ValuesReader reader)
    {
        byte[] buffer = input.readBytes();
        try {
            reader.initFromPage(0, ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, 0, buffer.length)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}