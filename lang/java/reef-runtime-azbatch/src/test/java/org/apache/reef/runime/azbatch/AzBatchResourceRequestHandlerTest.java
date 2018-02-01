/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.azbatch.driver;

import static org.mockito.Mockito.*;

/**
 * Tests for YarnResourceRequestHandler.
 */
public final class AzBatchResourceRequestHandlerTest {

    private final ResourceCatalog resourceCatalog = Mockito.mock(ResourceCatalog.class);
    private final AzureBatchResourceRequestHandler resourceRequestHandler =
            new AzureBatchResourceRequestHandler(azureBatchResourceManager);

    public void testEvaluatorCount() throws InjectionException {
        final LoggingScopeFactory loggingScopeFactory =
                Tang.Factory.getTang().newInjector().getInstance(LoggingScopeFactory.class);

        final EvaluatorRequestor evaluatorRequestor =
                new EvaluatorRequestorImpl(resourceCatalog, resourceRequestHandler, loggingScopeFactory);

    }
}
