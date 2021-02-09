package org.apache.cassandra.service;
/*
 *
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
 *
 */
<<<<<<< HEAD
=======

import org.apache.cassandra.gms.EchoMessage;
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EchoVerbHandler implements IVerbHandler<NoPayload>
{
    public static final EchoVerbHandler instance = new EchoVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(EchoVerbHandler.class);

    public void doVerb(Message<NoPayload> message)
    {
<<<<<<< HEAD
        logger.trace("Sending ECHO_RSP to {}", message.from());
        MessagingService.instance().send(message.emptyResponse(), message.from());
=======
        if (!StorageService.instance.isShutdown())
        {
            logger.trace("Sending a EchoMessage reply {}", message.from);
            MessageOut<EchoMessage> echoMessage = new MessageOut<EchoMessage>(MessagingService.Verb.REQUEST_RESPONSE, EchoMessage.instance, EchoMessage.serializer);
            MessagingService.instance().sendReply(echoMessage, id, message.from);
        }
        else
        {
            logger.trace("Not sending EchoMessage reply to {} - we are shutdown", message.from);
        }
>>>>>>> aa92e8868800460908717f1a1a9dbb7ac67d79cc
    }
}
