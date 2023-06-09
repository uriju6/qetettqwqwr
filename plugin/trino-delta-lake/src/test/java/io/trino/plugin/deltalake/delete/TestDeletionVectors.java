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

import com.google.common.io.Resources;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.OptionalInt;

import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.delete.DeletionVectors.Base85Codec.decodeBlocks;
import static io.trino.plugin.deltalake.delete.DeletionVectors.Base85Codec.decodeBytes;
import static io.trino.plugin.deltalake.delete.DeletionVectors.Base85Codec.encodeBytes;
import static io.trino.plugin.deltalake.delete.DeletionVectors.readDeletionVectors;
import static io.trino.plugin.deltalake.delete.DeletionVectors.toFileName;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeletionVectors
{
    @Test
    public void testUuidStorageType()
            throws Exception
    {
        // The deletion vector has a deleted row at position 1
        Path path = new File(Resources.getResource("databricks/deletion_vectors").toURI()).toPath();
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);

        RoaringBitmap[] bitmaps = readDeletionVectors(fileSystem, Location.of(path.toString()), "u", "R7QFX3rGXPFLhHGq&7g<", OptionalInt.of(1), 34, 1);
        assertThat(bitmaps).hasSize(1);
        assertFalse(bitmaps[0].contains(0));
        assertTrue(bitmaps[0].contains(1));
        assertFalse(bitmaps[0].contains(2));
    }

    @Test
    public void testUnsupportedPathStorageType()
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        assertThatThrownBy(() -> readDeletionVectors(fileSystem, Location.of("s3://bucket/table"), "p", "s3://bucket/table/deletion_vector.bin", OptionalInt.empty(), 40, 1))
                .hasMessageContaining("Unsupported storage type for deletion vector: p");
    }

    @Test
    public void testUnsupportedInlineStorageType()
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        assertThatThrownBy(() -> readDeletionVectors(fileSystem, Location.of("s3://bucket/table"), "i", "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L", OptionalInt.empty(), 40, 1))
                .hasMessageContaining("Unsupported storage type for deletion vector: i");
    }

    @Test
    public void testToFileName()
    {
        assertEquals(toFileName("R7QFX3rGXPFLhHGq&7g<"), "deletion_vector_a52eda8c-0a57-4636-814b-9c165388f7ca.bin");
        assertEquals(toFileName("ab^-aqEH.-t@S}K{vb[*k^"), "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin");
    }

    @Test
    public void testEncodeBytes()
    {
        // The test case comes from https://rfc.zeromq.org/spec/32
        byte[] inputBytes = new byte[] {(byte) 0x86, 0x4F, (byte) 0xD2, 0x6F, (byte) 0xB5, 0x59, (byte) 0xF7, 0x5B};
        String encoded = encodeBytes(inputBytes);
        assertEquals(encoded, "HelloWorld");
    }

    @Test
    public void testDecodeBytes()
    {
        String data = "HelloWorld";
        byte[] bytes = decodeBytes(data, 8);
        assertEquals(bytes, new byte[] {(byte) 0x86, 0x4F, (byte) 0xD2, 0x6F, (byte) 0xB5, 0x59, (byte) 0xF7, 0x5B});
    }

    @Test
    public void testDecodeBlocksIllegalCharacter()
    {
        assertThatThrownBy(() -> decodeBlocks("ab" + 0x7F + "de")).hasMessageContaining("Input should be 5 character aligned");

        assertThatThrownBy(() -> decodeBlocks("abîde")).hasMessageContaining("î is not valid Base85 character");
        assertThatThrownBy(() -> decodeBlocks("abπde")).hasMessageContaining("π is not valid Base85 character");
        assertThatThrownBy(() -> decodeBlocks("ab\"de")).hasMessageContaining("\" is not valid Base85 character");
    }

    @Test
    public void testCodecRoundTrip()
    {
        assertEquals("HelloWorld", encodeBytes(decodeBytes("HelloWorld", 8)));
        assertEquals("wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L", encodeBytes(decodeBytes("wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L", 40)));
    }
}
