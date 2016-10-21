package org.apache.hadoop.io.compress.zstd;

/**
 * Created by dillon on 8/30/16.
 */
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ZstdCompressor implements Compressor{

    private static final Log LOG = LogFactory.getLog(ZstdCompressor.class);

    private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;

    private long stream;
    private CompressionLevel level;
    private int directBufferSize;
    private Buffer compressedDirectBuf = null;
    private int uncompressedDirectBufLen;
    private Buffer uncompressedDirectBuf = null;
    private byte[] userBuf = null;
    private int userBufOff = 0, userBufLen = 0;
    private boolean finish, finished;
    private long bytesRead = 0L;
    private long bytesWritten = 0L;

    /**
     * The compression level for zstd library.
     */
    public static enum CompressionLevel {
        /**
         * Compression level for no compression.
         */
        NO_COMPRESSION (0),

        /**
         * Compression level for fastest compression.
         */
        BEST_SPEED (1),

        /**
         * Compression level 2.
         */
        TWO (2),


        /**
         * Compression level 3. (DEFAULT)
         */
        THREE (3),


        /**
         * Compression level 4.
         */
        FOUR (4),


        /**
         * Compression level 5.
         */
        FIVE (5),


        /**
         * Compression level 6.
         */
        SIX (6),

        /**
         * Compression level 7.
         */
        SEVEN (7),

        /**
         * Compression level 8.
         */
        EIGHT (8),

        /**
         * Compression level 9.
         */
        NINE (9),

        /**
         * Compression level 10.
         */
        TEN (10),

        /**
         * Compression level 11.
         */
        ELEVEN (11),

        /**
         * Compression level 12.
         */
        TWELVE (12),

        /**
         * Compression level 13.
         */
        THIRTEEN (13),

        /**
         * Compression level 14.
         */
        FOURTEEN (14),

        /**
         * Compression level 15.
         */
        FIFTEEN (15),

        /**
         * Compression level 16.
         */
        SIXTEEN (16),

        /**
         * Compression level 17.
         */
        SEVENTEEN (17),

        /**
         * Compression level 18.
         */
        EIGHTEEN (18),

        /**
         * Compression level for best compression.
         */
        BEST_COMPRESSION (19),

        /**
         * Default compression level.
         */
        DEFAULT_COMPRESSION (-1);


        private final int compressionLevel;

        CompressionLevel(int level) {
            compressionLevel = level;
        }

        int compressionLevel() {
            return compressionLevel;
        }
    };

    private static boolean nativeZstdLoaded = false;

    static {
        if (NativeCodeLoader.isNativeCodeLoaded()) {
            try {
                // Initialize the native library
                initIDs();
                nativeZstdLoaded = true;
            } catch (Throwable t) {
                LOG.error("failed to load native zstd library", t);
            }
        }
    }

    public static boolean isNativeCodeLoaded() {
        return nativeZstdLoaded;
    }

    /**
     * Creates a new compressor.
     *
     * @param directBufferSize size of the direct buffer to be used.
     */
    public ZstdCompressor(int directBufferSize) {
        this.directBufferSize = directBufferSize;

        uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        compressedDirectBuf.position(directBufferSize);
    }

    /**
     * Creates a new compressor with the default buffer size.
     */
    public ZstdCompressor() {
        this(DEFAULT_DIRECT_BUFFER_SIZE);
    }

    /**
     * Sets input data for compression.
     * This should be called whenever #needsInput() returns
     * <code>true</code> indicating that more input data is required.
     *
     * @param b Input data
     * @param off Start offset
     * @param len Length
     */
    @Override
    public void setInput(byte[] b, int off, int len){
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        finished = false;

        if (len > uncompressedDirectBuf.remaining()) {
            // save data; now !needsInput
            this.userBuf = b;
            this.userBufOff = off;
            this.userBufLen = len;
        } else {
            ((ByteBuffer) uncompressedDirectBuf).put(b, off, len);
            uncompressedDirectBufLen = uncompressedDirectBuf.position();
        }

        bytesRead += len;
    }

    /**
     * If a write would exceed the capacity of the direct buffers, it is set
     * aside to be loaded by this function while the compressed data are
     * consumed.
     */
    void setInputFromSavedData() {
        if (0 >= userBufLen) {
            return;
        }
        finished = false;

        uncompressedDirectBufLen = Math.min(userBufLen, directBufferSize);
        ((ByteBuffer) uncompressedDirectBuf).put(userBuf, userBufOff,
                uncompressedDirectBufLen);

        // Note how much data is being fed to zstd
        userBufOff += uncompressedDirectBufLen;
        userBufLen -= uncompressedDirectBufLen;
    }

    /**
     * Returns true if the input data buffer is empty and
     * #setInput() should be called to provide more input.
     *
     * @return <code>true</code> if the input data buffer is empty and
     * #setInput() should be called in order to provide more input.
     */
    @Override
    public boolean needsInput(){
        return !(compressedDirectBuf.remaining() > 0
                || uncompressedDirectBuf.remaining() == 0 || userBufLen > 0);
    }

    /**
     * Sets preset dictionary for compression. A preset dictionary
     * is used when the history buffer can be predetermined.
     *
     * @param b Dictionary data bytes
     * @param off Start offset
     * @param len Length
     */
    @Override
    public void setDictionary(byte[] b, int off, int len){
        //Do Nothing
    }

    /**
     * Return number of uncompressed bytes input so far.
     */
    @Override
    public long getBytesRead(){
        return bytesRead;
    }

    /**
     * Return number of compressed bytes output so far.
     */
    @Override
    public long getBytesWritten(){
        return bytesWritten;
    }

    /**
     * When called, indicates that compression should end
     * with the current contents of the input buffer.
     */
    @Override
    public void finish() {
        finish = true;
    }

    /**
     * Returns true if the end of the compressed
     * data output stream has been reached.
     * @return <code>true</code> if the end of the compressed
     * data output stream has been reached.
     */
    @Override
    public boolean finished(){
        // Check if all uncompressed data has been consumed
        return (finish && finished && compressedDirectBuf.remaining() == 0);
    }

    /**
     * Fills specified buffer with compressed data. Returns actual number
     * of bytes of compressed data. A return value of 0 indicates that
     * needsInput() should be called in order to determine if more input
     * data is required.
     *
     * @param b Buffer for the compressed data
     * @param off Start offset of the data
     * @param len Size of the buffer
     * @return The actual number of bytes of compressed data.
     */
    @Override
    public int compress(byte[] b, int off, int len) throws IOException{
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        // Check if there is compressed data
        int n = compressedDirectBuf.remaining();
        if (n > 0) {
            n = Math.min(n, len);
            ((ByteBuffer) compressedDirectBuf).get(b, off, n);
            bytesWritten += n;
            return n;
        }

        // Re-initialize the zstd's output direct-buffer
        compressedDirectBuf.clear();
        compressedDirectBuf.limit(0);
        if (0 == uncompressedDirectBuf.position()) {
            // No compressed data, so we should have !needsInput or !finished
            setInputFromSavedData();
            if (0 == uncompressedDirectBuf.position()) {
                // Called without data; write nothing
                finished = true;
                return 0;
            }
        }

        // Compress data
        n = compressBytesDirect();
        compressedDirectBuf.limit(n);
        uncompressedDirectBuf.clear(); // zstd consumes all buffer input

        // Set 'finished' if zstd has consumed all user-data
        if (0 == userBufLen) {
            finished = true;
        }

        // Get atmost 'len' bytes
        n = Math.min(n, len);
        bytesWritten += n;
        ((ByteBuffer) compressedDirectBuf).get(b, off, n);

        return n;
    }

    /**
     * Resets compressor so that a new set of input data can be processed.
     */
    @Override
    public void reset(){
        finish = false;
        finished = false;
        uncompressedDirectBuf.clear();
        uncompressedDirectBufLen = 0;
        compressedDirectBuf.clear();
        compressedDirectBuf.limit(0);
        userBufOff = userBufLen = 0;
        bytesRead = bytesWritten = 0L;
    }

    /**
     * Closes the compressor and discards any unprocessed input.
     */
    @Override
    public void end(){
    }

    /**
     * Prepare the compressor to be used in a new stream with settings defined in
     * the given Configuration
     *
     * @param conf Configuration from which new setting are fetched
     */
    @Override
    public void reinit(Configuration conf){
        reset();
    }


    private native static void initIDs();
    private native int compressBytesDirect();

    public native static String getLibraryName();
}

