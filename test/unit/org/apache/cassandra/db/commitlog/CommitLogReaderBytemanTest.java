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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.File;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Tests how {@link CommitLogReader#filterCommitLogFiles(File[])} handles an {@link java.io.IOError} thrown
 * while a segment header is read. {@link FSReadError} extends {@link java.io.IOError}, so it is not an
 * {@link Exception} and needs handling of its own.
 */
@RunWith(BMUnitRunner.class)
public class CommitLogReaderBytemanTest
{
    private static final AtomicInteger injected = new AtomicInteger();

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void before()
    {
        injected.set(0);
    }

    /**
     * Thrown by a byteman rule. Reports a read on a file that another process removed. The class exists
     * because a byteman rule can only construct an exception through a constructor whose parameter types
     * match the call exactly.
     */
    public static class MissingSegmentError extends FSReadError
    {
        public MissingSegmentError()
        {
            super(new NoSuchFileException("segment"), "segment");
            injected.incrementAndGet();
        }
    }

    /**
     * Thrown by a byteman rule. Reports a read failure that is not a missing file.
     */
    public static class CorruptSegmentError extends FSReadError
    {
        public CorruptSegmentError()
        {
            super(new IOException("corrupt"), "segment");
            injected.incrementAndGet();
        }
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Segment file disappears while the header is read",
                              targetClass = "CommitLogDescriptor",
                              targetMethod = "readHeader",
                              action = "throw new org.apache.cassandra.db.commitlog.CommitLogReaderBytemanTest$MissingSegmentError()") })
    public void testFileThatDisappearsIsLeftToRecover() throws IOException
    {
        File file = createSegmentFile();
        List<File> filtered = CommitLogReader.filterCommitLogFiles(new File[]{ file });
        assertThat(injected.get()).isEqualTo(1);
        assertThat(filtered).containsExactly(file);
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Segment header read fails on a corrupt file",
                              targetClass = "CommitLogDescriptor",
                              targetMethod = "readHeader",
                              action = "throw new org.apache.cassandra.db.commitlog.CommitLogReaderBytemanTest$CorruptSegmentError()") })
    public void testOtherReadErrorsFailTheReplay() throws IOException
    {
        File file = createSegmentFile();
        assertThatExceptionOfType(FSReadError.class)
        .isThrownBy(() -> CommitLogReader.filterCommitLogFiles(new File[]{ file }))
        .withCauseInstanceOf(IOException.class);
        assertThat(injected.get()).isEqualTo(1);
    }

    private static File createSegmentFile() throws IOException
    {
        File file = new File(Files.createTempFile("CommitLog-", ".log"));
        file.deleteOnExit();
        Files.write(file.toPath(), new byte[64]);
        return file;
    }
}
