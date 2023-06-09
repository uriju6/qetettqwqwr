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

import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;

import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.ROW_ID_COLUMN_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public final class PositionDeleteFilter
{
    private final RoaringBitmap[] deletedRows;

    public PositionDeleteFilter(RoaringBitmap[] deletedRows)
    {
        this.deletedRows = requireNonNull(deletedRows, "deletedRows is null");
    }

    public RowPredicate createPredicate(List<DeltaLakeColumnHandle> columns)
    {
        int filePositionChannel = rowPositionChannel(columns);
        return (page, position) -> {
            long filePosition = BIGINT.getLong(page.getBlock(filePositionChannel), position);
            return !DeletionVectors.contains(deletedRows, filePosition);
        };
    }

    private static int rowPositionChannel(List<DeltaLakeColumnHandle> columns)
    {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getBaseColumnName().equals(ROW_ID_COLUMN_NAME)) {
                return i;
            }
        }
        throw new IllegalArgumentException("No row position column");
    }
}
