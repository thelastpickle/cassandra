/*
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

package org.apache.cassandra.diag;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DummyEvent extends DiagnosticEvent
{
    private final DummyEventType type;
    private final String msg;

    private final static Random rnd = new Random(0);

    public enum DummyEventType
    {
        TYPE_A, TYPE_B
    }

    public DummyEvent()
    {
        this.type = rnd.nextBoolean() ? DummyEventType.TYPE_A : DummyEventType.TYPE_B;

        int bytes = rnd.nextInt(11)+1;
        char[] ch = new char[bytes];
        for (int i = 0; i < bytes; i++)
            ch[i] = (char) (32 + rnd.nextInt(95));
        this.msg = new String(ch);
    }

    public static DummyEvent create()
    {
        return new DummyEvent();
    }

    @Override
    public Enum<?> getType()
    {
        return type;
    }

    @Override
    public Object getSource()
    {
        return DummyEventEmitter.instance();
    }

    @Override
    public Map<String, Serializable> toMap()
    {
        Map<String, Serializable> ret = new HashMap<>();
        ret.put("content", msg);
        ret.put("val1", rnd.nextInt());
        ret.put("val2", rnd.nextLong());
        return ret;
    }
}
