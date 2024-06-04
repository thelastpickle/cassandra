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

package org.apache.cassandra.service.paxos.v1;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Type;

public class ProposeVerbHandler implements IVerbHandler<Commit>
{
    public static final ProposeVerbHandler instance = new ProposeVerbHandler();

    public static Boolean doPropose(Commit proposal)
    {
        return PaxosState.legacyPropose(proposal);
    }

    public void doVerb(Message<Commit> message)
    {
        // Initialize the sensor and set ExecutorLocals
        RequestSensors sensors = new RequestSensors();
        Context context = Context.from(message.payload.update.metadata());
        // Propose phase consults the Paxos table for more recent promises, so a read sensor is registered in addition to the write sensor
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        RequestTracker.instance.set(sensors);

        Message.Builder<Boolean> reply = message.responseWithBuilder(doPropose(message.payload));
        SensorsCustomParams.addWriteSensorToResponse(reply, sensors, context);
        SensorsCustomParams.addReadSensorToResponse(reply, sensors, context);
        MessagingService.instance().send(reply.build(), message.from());
    }
}
