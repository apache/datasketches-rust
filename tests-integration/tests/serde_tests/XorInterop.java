// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.datasketches.filters.xorfilter.XorFilter;
import org.apache.datasketches.filters.xorfilter.XorFilterBuilder;

class XorInterop {
  public static void main(String[] args) throws Exception {
    Path directory = Path.of(args[0]);
    for (int bits : new int[] {8, 16}) {
      for (int count : new int[] {0, 1, 4, 16384, 50001}) {
        String name = bits + "_" + count + ".sk";
        byte[] image = Files.readAllBytes(directory.resolve("rust_" + name));
        XorFilter restored = XorFilter.heapify(MemorySegment.ofArray(image));
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
          values[i] = i == 3 ? Long.MIN_VALUE : i + 1;
          if (!restored.query(values[i])) {
            throw new AssertionError("false negative: " + name + ", value " + values[i]);
          }
        }
        if (count == 0 && restored.query(1L)) {
          throw new AssertionError("empty filter matched: " + name);
        }
        Files.write(directory.resolve("java_" + name),
            XorFilterBuilder.create(values, bits, 42).toByteArray());
        Files.write(directory.resolve("roundtrip_" + name), restored.toByteArray());
      }
    }
  }
}
