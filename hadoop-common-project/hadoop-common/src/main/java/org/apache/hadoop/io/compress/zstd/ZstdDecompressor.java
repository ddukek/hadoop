package org.apache.hadoop.io.compress.zstd;

/**
 * Created by dillon on 8/30/16.
 */

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;

public class ZstdDecompressor implements Decompressor {
    private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;
    private static final Log LOG =
            LogFactory.getLog(ZstdDecompressor.class.getName());

    private int directBufferSize;
    private Buffer compressedDirectBuf = null;
    private int compressedDirectBufLen;
    private Buffer uncompressedDirectBuf = null;
    private byte[] userBuf = null;
    private int userBufOff = 0, userBufLen = 0;
    private boolean finished;

    private static boolean nativeZstdLoaded = false;

    static {
        if (NativeCodeLoader.isNativeCodeLoaded()) {
            try {
                initIDs();
                nativeZstdLoaded = true;
            } catch (Throwable t) {
                LOG.error("failed to load ZstdDecompressor", t);
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
    public ZstdDecompressor(int directBufferSize) {
        this.directBufferSize = directBufferSize;

        compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);

    }

    /**
     * Creates a new decompressor with the default buffer size.
     */
    public ZstdDecompressor() {
        this(DEFAULT_DIRECT_BUFFER_SIZE);
    }

    /**
     * Sets input data for decompression.
     * This should be called if and only if {@link #needsInput()} returns
     * <code>true</code> indicating that more input data is required.
     * (Both native and non-native versions of various Decompressors require
     * that the data passed in via <code>b[]</code> remain unmodified until
     * the caller is explicitly notified--via {@link #needsInput()}--that the
     * buffer may be safely modified.  With this requirement, an extra
     * buffer-copy can be avoided.)
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

        this.userBuf = b;
        this.userBufOff = off;
        this.userBufLen = len;

        setInputFromSavedData();

        // Reinitialize snappy's output direct-buffer
        uncompressedDirectBuf.limit(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
    }

    /**
     * If a write would exceed the capacity of the direct buffers, it is set
     * aside to be loaded by this function while the compressed data are
     * consumed.
     */
    void setInputFromSavedData() {
        compressedDirectBufLen = Math.min(userBufLen, directBufferSize);

        // Reinitialize zstd's input direct buffer
        compressedDirectBuf.rewind();
        ((ByteBuffer) compressedDirectBuf).put(userBuf, userBufOff,
                compressedDirectBufLen);

        // Note how much data is being fed to zstd
        userBufOff += compressedDirectBufLen;
        userBufLen -= compressedDirectBufLen;
    }

    /**
     * Returns <code>true</code> if the input data buffer is empty and
     * {@link #setInput(byte[], int, int)} should be called to
     * provide more input.
     *
     * @return <code>true</code> if the input data buffer is empty and
     * {@link #setInput(byte[], int, int)} should be called in
     * order to provide more input.
     */
    @Override
    public boolean needsInput(){
        // Consume remaining compressed data?
        if (uncompressedDirectBuf.remaining() > 0) {
            return false;
        }

        // Check if snappy has consumed all input
        if (compressedDirectBufLen <= 0) {
            // Check if we have consumed all user-input
            if (userBufLen <= 0) {
                return true;
            } else {
                setInputFromSavedData();
            }
        }

        return false;
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
        //Do nothing
    }

    /**
     * Returns <code>true</code> if a preset dictionary is needed for decompression.
     * @return <code>true</code> if a preset dictionary is needed for decompression
     */
    @Override
    public boolean needsDictionary(){
        return false;
    }

    /**
     * Returns <code>true</code> if the end of the decompressed
     * data output stream has been reached. Indicates a concatenated data stream
     * when finished() returns <code>true</code> and {@link #getRemaining()}
     * returns a positive value. finished() will be reset with the
     * {@link #reset()} method.
     * @return <code>true</code> if the end of the decompressed
     * data output stream has been reached.
     */
    @Override
    public boolean finished(){
        return (finished && uncompressedDirectBuf.remaining() == 0);
    }

    /**
     * Fills specified buffer with uncompressed data. Returns actual number
     * of bytes of uncompressed data. A return value of 0 indicates that
     * {@link #needsInput()} should be called in order to determine if more
     * input data is required.
     *
     * @param b Buffer for the compressed data
     * @param off Start offset of the data
     * @param len Size of the buffer
     * @return The actual number of bytes of compressed data.
     * @throws IOException
     */
    @Override
    public int decompress(byte[] b, int off, int len) throws IOException{
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        int n = 0;

        // Check if there is uncompressed data
        n = uncompressedDirectBuf.remaining();
        if (n > 0) {
            n = Math.min(n, len);
            ((ByteBuffer) uncompressedDirectBuf).get(b, off, n);
            return n;
        }
        if (compressedDirectBufLen > 0) {
            // Re-initialize the snappy's output direct buffer
            uncompressedDirectBuf.rewind();
            uncompressedDirectBuf.limit(directBufferSize);

            // Decompress data
            n = decompressBytesDirect();
            uncompressedDirectBuf.limit(n);

            if (userBufLen <= 0) {
                finished = true;
            }

            // Get atmost 'len' bytes
            n = Math.min(n, len);
            ((ByteBuffer) uncompressedDirectBuf).get(b, off, n);
        }

        return n;
    }

    /**
     * Returns the number of bytes remaining in the compressed data buffer.
     * Indicates a concatenated data stream if {@link #finished()} returns
     * <code>true</code> and getRemaining() returns a positive value. If
     * {@link #finished()} returns <code>true</code> and getRemaining() returns
     * a zero value, indicates that the end of data stream has been reached and
     * is not a concatenated data stream.
     * @return The number of bytes remaining in the compressed data buffer.
     */
    @Override
    public int getRemaining(){
        // Never use this function in BlockDecompressorStream.
        return 0;
    }

    /**
     * Resets decompressor and input and output buffers so that a new set of
     * input data can be processed. If {@link #finished()}} returns
     * <code>true</code> and {@link #getRemaining()} returns a positive value,
     * reset() is called before processing of the next data stream in the
     * concatenated data stream. {@link #finished()} will be reset and will
     * return <code>false</code> when reset() is called.
     */
    @Override
    public void reset(){
        finished = false;
        compressedDirectBufLen = 0;
        uncompressedDirectBuf.limit(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
        userBufOff = userBufLen = 0;
    }

    /**
     * Closes the decompressor and discards any unprocessed input.
     */
    @Override
    public void end(){
        // do nothing
    }

    private native static void initIDs();
    private native int decompressBytesDirect();

    int decompressDirect(ByteBuffer src, ByteBuffer dst) throws IOException {
        assert (this instanceof ZstdDirectDecompressor);

        ByteBuffer presliced = dst;
        if (dst.position() > 0) {
            presliced = dst;
            dst = dst.slice();
        }

        Buffer originalCompressed = compressedDirectBuf;
        Buffer originalUncompressed = uncompressedDirectBuf;
        int originalBufferSize = directBufferSize;
        compressedDirectBuf = src.slice();
        compressedDirectBufLen = src.remaining();
        uncompressedDirectBuf = dst;
        directBufferSize = dst.remaining();
        int n = 0;
        try {
            n = decompressBytesDirect();
            presliced.position(presliced.position() + n);
            // SNAPPY always consumes the whole buffer or throws an exception
            src.position(src.limit());
            finished = true;
        } finally {
            compressedDirectBuf = originalCompressed;
            uncompressedDirectBuf = originalUncompressed;
            compressedDirectBufLen = 0;
            directBufferSize = originalBufferSize;
        }
        return n;
    }

    public static class ZstdDirectDecompressor extends ZstdDecompressor implements
            DirectDecompressor {

        @Override
        public boolean finished() {
            return (endOfInput && super.finished());
        }

        @Override
        public void reset() {
            super.reset();
            endOfInput = true;
        }

        private boolean endOfInput;

        @Override
        public void decompress(ByteBuffer src, ByteBuffer dst)
                throws IOException {
            assert dst.isDirect() : "dst.isDirect()";
            assert src.isDirect() : "src.isDirect()";
            assert dst.remaining() > 0 : "dst.remaining() > 0";
            this.decompressDirect(src, dst);
            endOfInput = !src.hasRemaining();
        }

        @Override
        public void setDictionary(byte[] b, int off, int len) {
            throw new UnsupportedOperationException(
                    "byte[] arrays are not supported for DirectDecompressor");
        }

        @Override
        public int decompress(byte[] b, int off, int len) {
            throw new UnsupportedOperationException(
                    "byte[] arrays are not supported for DirectDecompressor");
        }
    }

}