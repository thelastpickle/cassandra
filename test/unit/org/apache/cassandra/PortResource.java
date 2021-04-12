
package org.apache.cassandra;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.TimeUnit;

import org.junit.rules.ExternalResource;

import org.apache.cassandra.io.util.FileUtils;
import org.awaitility.Awaitility;
import org.awaitility.pollinterval.FibonacciPollInterval;


public class PortResource extends ExternalResource
{
    private final String host;
    private final int port;
    private File reserved;
    private FileLock lock;

    public PortResource(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    @Override
    protected void before() throws Throwable
    {
        // Wait until the ports for the test are available. Useful when ran on CI with parallel tests running
        Awaitility.with().pollInterval(FibonacciPollInterval.fibonacci())
                .await().atMost(5, TimeUnit.MINUTES)
                .until(() ->
                {
                    try (ServerSocket ignored = new ServerSocket(port, 0, InetAddress.getByName(host)))
                    {
                        ignored.setReuseAddress(true);
                        reserved = new File(FileUtils.getTempDir(), "cassandra-test-reserve-" + host + "-" + port);
                        try (RandomAccessFile file = new RandomAccessFile(reserved, "rw"))
                        {
                            lock = file.getChannel().lock();
                            reserved.deleteOnExit();
                            return true;
                        }
                    }
                    catch (IOException ignoredException) {}
                    if (null != reserved) reserved.delete();
                    return false;
                });
    }

    @Override
    protected void after()
    {
        try
        {
            lock.channel().close();
            lock.release();
        }
        catch (IOException ignoredException) {}
        reserved.delete();
    }
}
