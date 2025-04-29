package org.acme.util;

import lombok.Getter;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * {@code RewindableFileInputStream} is a {@code FilterInputStream} wrapper over a {@code FileInputStream}
 * that provides reliable {@code mark()} and {@code reset()} support backed by a seekable {@code FileChannel}.
 * Unlike standard buffered streams, it can reset to the marked position or optionally rewind to the start
 * of the file after a full read, depending on configuration.
 * <p><strong>Note:</strong> The {@code readlimit} parameter passed to {@code mark()} is ignored,
 * because file-based streams can seek freely and are not constrained by buffered memory.</p>
 */
public class RewindableFileInputStream extends FilterInputStream {

    @Getter
    private final FileChannel fileChannel;
    private final long fileSize;
    private long markPosition = 0;
    private final boolean autoRewindOnReset;

    /**
     * Constructs a {@code RewindableFileInputStream} with explicit control over auto-rewind behavior.
     *
     * @param fis                The {@code FileInputStream} to wrap.
     * @param autoRewindOnReset   If {@code true}, calling {@code reset()} after reading to EOF will rewind
     *                            the stream to the beginning automatically. If {@code false}, {@code reset()}
     *                            always returns to the last marked position.
     */
    public RewindableFileInputStream(FileInputStream fis, boolean autoRewindOnReset) {
        super(fis);
        try {
            this.fileChannel = fis.getChannel();
            this.fileSize = fileChannel.size(); // Pre-fetch file size once
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file channel", e);
        }
        this.autoRewindOnReset = autoRewindOnReset;
    }

    /**
     * Constructs a {@code RewindableFileInputStream} with auto-rewind disabled.
     */
    public RewindableFileInputStream(FileInputStream fis) {
        this(fis, false);
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        try {
            this.markPosition = fileChannel.position();
        } catch (IOException e) {
            throw new RuntimeException("Failed to mark file position", e);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        if (autoRewindOnReset) {
            long currentPosition = fileChannel.position();

            if (currentPosition >= fileSize) {
                fileChannel.position(0);
                return;
            }
        }

        fileChannel.position(markPosition);
    }

}
