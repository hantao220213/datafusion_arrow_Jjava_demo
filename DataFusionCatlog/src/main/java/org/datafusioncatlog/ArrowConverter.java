package org.datafusioncatlog;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ArrowConverter {
    private final BufferAllocator allocator;

    public ArrowConverter() {
        this.allocator = new RootAllocator();
    }

    public IntVector convertToIntVector(int value) {
        IntVector vector = new IntVector("column_name", allocator);
        vector.allocateNew(1);
        vector.setSafe(0, value);
        vector.setValueCount(1);
        return vector;
    }

    // 可以根据需要实现其他转换方法

    public void writeVectorToFile(FieldVector vector, String filePath) throws IOException {
        //Schema schema = new Schema(new Field("column_name", true, ArrowType.Int.INSTANCE, null));
        List<Field> fields = new Field("column_name", true, ArrowType.Int.INSTANCE, null);

        VectorSchemaRoot root = new VectorSchemaRoot(fields, vector);
        File file = new File(filePath);

        try (ArrowFileWriter writer = new ArrowFileWriter(root, null, file)) {
            writer.writeBatch();
        }
    }
}
