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
package io.trino.plugin.deltalake.delete;

import com.google.common.base.CharMatcher;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.TrinoDataInputStream;
import io.trino.spi.TrinoException;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-format
public final class DeletionVectors
{
    private static final Logger log = Logger.get(DeletionVectors.class);
    private static final int PORTABLE_ROARING_BITMAP_MAGIC_NUMBER = 1681511377;

    private static final String UUID_MARKER = "u"; // relative path with random prefix on disk
    private static final String PATH_MARKER = "p"; // absolute path on disk
    private static final String INLINE_MARKER = "i"; // inline

    private static final CharMatcher ALPHANUMERIC = CharMatcher.inRange('A', 'Z').or(CharMatcher.inRange('a', 'z')).or(CharMatcher.inRange('0', '9')).precomputed();

    private DeletionVectors() {}

    public static RoaringBitmap[] readDeletionVectors(
            TrinoFileSystem fileSystem,
            Location location,
            String storageType,
            String pathOrInlineDv,
            OptionalInt offset,
            int sizeInBytes,
            long cardinality)
            throws IOException
    {
        if (storageType.equals(UUID_MARKER)) {
            TrinoInputFile inputFile = fileSystem.newInputFile(location.appendPath(toFileName(pathOrInlineDv)));
            ByteBuffer buffer = readDeletionVector(inputFile, offset.orElseThrow(), sizeInBytes);
            RoaringBitmap[] bitmaps = deserializeDeletionVectors(buffer);
            if (bitmaps.length != cardinality) {
                // Don't throw an exception because Databricks may report the wrong cardinality when there are many deleted rows
                log.debug("The number of deleted rows expects %s but got %s", cardinality, bitmaps.length);
            }
            return bitmaps;
        }
        else if (storageType.equals(INLINE_MARKER) || storageType.equals(PATH_MARKER)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported storage type for deletion vector: " + storageType);
        }
        throw new IllegalArgumentException("Unexpected storage type: " + storageType);
    }

    public static String toFileName(String pathOrInlineDv)
    {
        int randomPrefixLength = pathOrInlineDv.length() - Base85Codec.ENCODED_UUID_LENGTH;
        String randomPrefix = pathOrInlineDv.substring(0, randomPrefixLength);
        checkArgument(ALPHANUMERIC.matchesAllOf(randomPrefix), "Random prefix must be alphanumeric: %s", randomPrefix);
        String prefix = randomPrefix.isEmpty() ? "" : randomPrefix + "/";
        String encodedUuid = pathOrInlineDv.substring(randomPrefixLength);
        UUID uuid = Base85Codec.decodeUuid(encodedUuid);
        return "%sdeletion_vector_%s.bin".formatted(prefix, uuid);
    }

    public static ByteBuffer readDeletionVector(TrinoInputFile inputFile, int offset, int expectedSize)
            throws IOException
    {
        byte[] bytes = new byte[expectedSize];
        TrinoDataInputStream inputStream = new TrinoDataInputStream(inputFile.newStream());
        inputStream.seek(offset);
        int actualSize = inputStream.readInt();
        if (actualSize != expectedSize) {
            // TODO: Investigate why these size differ
            log.warn("The size of deletion vector %s expects %s but got %s", inputFile.location(), expectedSize, actualSize);
        }
        inputStream.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN);
    }

    public static boolean contains(RoaringBitmap[] bitmaps, long value)
    {
        int high = highBytes(value);
        if (high >= bitmaps.length) {
            return false;
        }
        RoaringBitmap highBitmap = bitmaps[high];
        int low = lowBytes(value);
        return highBitmap.contains(low);
    }

    private static int highBytes(long value)
    {
        return toIntExact(value >> 32);
    }

    private static int lowBytes(long value)
    {
        return toIntExact(value);
    }

    public static RoaringBitmap[] deserializeDeletionVectors(ByteBuffer buffer)
            throws IOException
    {
        checkArgument(buffer.order() == LITTLE_ENDIAN, "Byte order must be little endian: %s", buffer.order());
        int magicNumber = buffer.getInt();
        if (magicNumber == PORTABLE_ROARING_BITMAP_MAGIC_NUMBER) {
            int size = toIntExact(buffer.getLong());
            RoaringBitmap[] bitmaps = new RoaringBitmap[size];
            for (int i = 0; i < size; i++) {
                int key = buffer.getInt();
                checkArgument(key >= 0);
                RoaringBitmap bitmap = new RoaringBitmap();
                bitmap.deserialize(buffer);
                bitmaps[i] = bitmap;
                buffer.position(buffer.position() + bitmap.serializedSizeInBytes());
            }
            return bitmaps;
        }
        throw new IllegalArgumentException("Unsupported magic number: " + magicNumber);
    }

    // This implements Base85 using the 4 byte block aligned encoding and character set from Z85 https://rfc.zeromq.org/spec/32
    // Delta Lake implementation is https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/util/Codec.scala
    static final class Base85Codec
    {
        private static final long BASE = 85L;
        private static final long BASE_2ND_POWER = 7225L; // 85^2
        private static final long BASE_3RD_POWER = 614125L; // 85^3
        private static final long BASE_4TH_POWER = 52200625L; // 85^4
        private static final int ASCII_BITMASK = 0x7F;

        // UUIDs always encode into 20 characters
        static final int ENCODED_UUID_LENGTH = 20;

        private static final String BASE85_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

        private static final byte[] ENCODE_MAP = BASE85_CHARACTERS.getBytes(UTF_8);
        // The bitmask is the same as largest possible value, so the length of the array must be one greater.
        private static final byte[] DECODE_MAP = new byte[ASCII_BITMASK + 1];

        static {
            Arrays.fill(DECODE_MAP, (byte) -1);
            for (int i = 0; i < ENCODE_MAP.length; i++) {
                DECODE_MAP[ENCODE_MAP[i]] = (byte) i;
            }
        }

        private Base85Codec() {}

        // This method will be used when supporting https://github.com/trinodb/trino/issues/17063
        // This is used for testing codec round trip for now
        public static String encodeBytes(byte[] input)
        {
            if (input.length % 4 == 0) {
                return encodeBlocks(ByteBuffer.wrap(input));
            }
            int alignedLength = ((input.length + 4) / 4) * 4;
            ByteBuffer buffer = ByteBuffer.allocate(alignedLength);
            buffer.put(input);
            while (buffer.hasRemaining()) {
                buffer.put((byte) 0);
            }
            buffer.rewind();
            return encodeBlocks(buffer);
        }

        private static String encodeBlocks(ByteBuffer buffer)
        {
            checkArgument(buffer.remaining() % 4 == 0);
            int numBlocks = buffer.remaining() / 4;
            // Every 4 byte block gets encoded into 5 bytes/chars
            int outputLength = numBlocks * 5;
            byte[] output = new byte[outputLength];
            int outputIndex = 0;

            while (buffer.hasRemaining()) {
                long sum = buffer.getInt() & 0x00000000ffffffffL;
                output[outputIndex] = ENCODE_MAP[(int) (sum / BASE_4TH_POWER)];
                sum %= BASE_4TH_POWER;
                output[outputIndex + 1] = ENCODE_MAP[(int) (sum / BASE_3RD_POWER)];
                sum %= BASE_3RD_POWER;
                output[outputIndex + 2] = ENCODE_MAP[(int) (sum / BASE_2ND_POWER)];
                sum %= BASE_2ND_POWER;
                output[outputIndex + 3] = ENCODE_MAP[(int) (sum / BASE)];
                output[outputIndex + 4] = ENCODE_MAP[(int) (sum % BASE)];
                outputIndex += 5;
            }
            return new String(output, US_ASCII);
        }

        public static ByteBuffer decodeBlocks(String encoded)
        {
            char[] input = encoded.toCharArray();
            checkArgument(input.length % 5 == 0, "Input should be 5 character aligned");
            ByteBuffer buffer = ByteBuffer.allocate(input.length / 5 * 4);

            int inputIndex = 0;
            while (buffer.hasRemaining()) {
                long sum = 0;
                sum += decodeInputChar(input[inputIndex]) * BASE_4TH_POWER;
                sum += decodeInputChar(input[inputIndex + 1]) * BASE_3RD_POWER;
                sum += decodeInputChar(input[inputIndex + 2]) * BASE_2ND_POWER;
                sum += decodeInputChar(input[inputIndex + 3]) * BASE;
                sum += decodeInputChar(input[inputIndex + 4]);
                buffer.putInt((int) sum);
                inputIndex += 5;
            }
            buffer.rewind();
            return buffer;
        }

        public static UUID decodeUuid(String encoded)
        {
            ByteBuffer buffer = decodeBlocks(encoded);
            return uuidFromByteBuffer(buffer);
        }

        private static UUID uuidFromByteBuffer(ByteBuffer buffer)
        {
            checkArgument(buffer.remaining() >= 16);
            long highBits = buffer.getLong();
            long lowBits = buffer.getLong();
            return new UUID(highBits, lowBits);
        }

        // This method will be used when supporting https://github.com/trinodb/trino/issues/17063
        // This is used for testing codec round trip for now
        public static byte[] decodeBytes(String encoded, int outputLength)
        {
            ByteBuffer result = decodeBlocks(encoded);
            if (result.remaining() > outputLength) {
                // Only read the expected number of bytes
                byte[] output = new byte[outputLength];
                result.get(output);
                return output;
            }
            return result.array();
        }

        private static long decodeInputChar(char chr)
        {
            checkArgument(BASE85_CHARACTERS.contains(String.valueOf(chr)), "%s is not valid Base85 character", chr);
            return DECODE_MAP[chr & ASCII_BITMASK];
        }
    }
}
