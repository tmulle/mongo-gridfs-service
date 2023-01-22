package org.acme.util;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Used to reset a fileinput stream so we can reread
 * from the stream without having to create a new stream
 *
 */
public class MarkableFileInputStream extends FilterInputStream {
    private FileChannel myFileChannel;
    private long mark = -1;

    public MarkableFileInputStream(FileInputStream fis) {
        super(fis);
        myFileChannel = fis.getChannel();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        mark = readlimit;
    }

    @Override
    public synchronized void reset() throws IOException {
        myFileChannel.position(mark);
    }
}