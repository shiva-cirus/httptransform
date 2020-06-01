/*
 * Copyright © 2016-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.http;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("url", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("e", Schema.of(Schema.Type.STRING)));
  @Test
  public void testMyTransform() throws Exception {
    HttpGetTransform.Conf config = new HttpGetTransform.Conf("url", INPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new HttpGetTransform(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("url", "https://jsonplaceholder.typicode.com/posts")
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
  }







}
