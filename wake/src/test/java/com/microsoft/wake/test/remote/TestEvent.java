/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.test.remote;

import java.io.Serializable;

public class TestEvent implements Serializable {

  private static final long serialVersionUID = 1L;
  private String message;

  private double load;
  
  public TestEvent(String message, double load) {
    this.message = message;
    this.load = load;
  }
  
  public String getMessage() {
    return message;
  }
  
  public double getLoad() {
    return load;
  }
  
  public String toString() {
    StringBuilder builder = new StringBuilder()
      .append("message=")
      .append(message)
      .append(" load=")
      .append(load);
    return builder.toString();
  }
}
