/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hama.bm.self.msgType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Objects;

/**
 * TextPair class for use in BipartiteMatching algorithm.
 *
 */
public final class ValuePair<E extends Writable,T extends Writable> implements Writable{
  
  E first;
  T second;
  
  String nameFirst = "First";
  String nameSecond = "Second";

  @SuppressWarnings("unchecked")
public ValuePair() {
	  first=(E) new IntWritable();
	  second=(T) new Text();
  }
  
  public ValuePair(E first, T second){
    this.first  = first;
    this.second = second;
  }
  
  /**
   * Sets the names of the attributes 
   */
  public ValuePair<E,T> setNames(String nameFirst, String nameSecond){
    this.nameFirst = nameFirst;
    this.nameSecond = nameSecond;
    return this;
  }
  
  

  public E getFirst() {
    return first;
  }

  public void setFirst(E first) {
    this.first = first;
  }

  public T getSecond() {
    return second;
  }

  public void setSecond(T second) {
    this.second = second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }
  
  @Override
  public String toString(){
    return Objects.toStringHelper(this)
        .add(nameFirst, getFirst())
        .add(nameSecond, getSecond())
        .toString();
  }

}
