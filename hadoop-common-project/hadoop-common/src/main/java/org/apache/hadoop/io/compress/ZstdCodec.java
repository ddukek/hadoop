package org.apache.hadoop.io.compress;

/**
 * Created by dillon on 8/30/16.
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.zstd.ZstdCompressor;
import org.apache.hadoop.io.compress.zstd.ZstdDecompressor;
import org.apache.hadoop.io.compress.zstd.ZstdDecompressor.ZstdDirectDecompressor;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.NativeCodeLoader;

public class ZstdCodec implements Configurable, CompressionCodec, DirectDecompressionCodec {
    Configuration conf;

    /**
     * Set the configuration to be used by this object.
     *
     * @param conf the configuration object.
     */
    @Override
    public void setConf(Configuration conf) {this.conf = conf;}

    /**
     * Return the configuration used by this object.
     *
     * @return the configuration object used by this objec.
     */
    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * Are the native zstd libraries loaded & initialized?
     */
    public static void checkNativeCodeLoaded() {
        if (!NativeCodeLoader.isNativeCodeLoaded()) {
            throw new RuntimeException("native zstd library not available: " +
                    "this version of libhadoop was built without " +
                    "zstd support.");
        }
        if (!ZstdCompressor.isNativeCodeLoaded()) {
            throw new RuntimeException("native zstd library not available: " +
                    "ZstdCompressor has not been loaded.");
        }
        if (!ZstdDecompressor.isNativeCodeLoaded()) {
            throw new RuntimeException("native zstd library not available: " +
                    "ZstdDecompressor has not been loaded.");
        }
    }

    public static boolean isNativeCodeLoaded() {
        return ZstdCompressor.isNativeCodeLoaded() &&
                ZstdDecompressor.isNativeCodeLoaded();
    }

    public static String getLibraryName() {
        return ZstdCompressor.getLibraryName();
    }

    /**
     * Create a {@link CompressionOutputStream} that will write to the given
     * {@link OutputStream}.
     *
     * @param out the location for the final output stream
     * @return a stream the user can write uncompressed data to have it compressed
     * @throws IOException
     */
    @Override
    public CompressionOutputStream createOutputStream(OutputStream out)
            throws IOException {
        return CompressionCodec.Util.
                createOutputStreamWithCodecPool(this, conf, out);
    }

    /**
     * Create a {@link CompressionOutputStream} that will write to the given
     * {@link OutputStream} with the given {@link Compressor}.
     *
     * @param out        the location for the final output stream
     * @param compressor compressor to use
     * @return a stream the user can write uncompressed data to have it compressed
     * @throws IOException
     */
    @Override
    public CompressionOutputStream createOutputStream(OutputStream out,
                                                      Compressor compressor)
            throws IOException {
        checkNativeCodeLoaded();
        int bufferSize = conf.getInt(
                CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFERSIZE_KEY,
                CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFERSIZE_DEFAULT);

        int compressionOverhead = (bufferSize / 6) + 32;

        return new BlockCompressorStream(out, compressor, bufferSize,
                compressionOverhead);
    }

    /**
     * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
     *
     * @return the type of compressor needed by this codec.
     */
    @Override
    public Class<? extends Compressor> getCompressorType() {
        checkNativeCodeLoaded();
        return ZstdCompressor.class;
    }

    /**
     * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
     *
     * @return a new compressor for use by this codec
     */
    @Override
    public Compressor createCompressor() {
        checkNativeCodeLoaded();
        int bufferSize = conf.getInt(
                CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFERSIZE_KEY,
                CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFERSIZE_DEFAULT);
        return new ZstdCompressor(bufferSize);
    }

    /**
     * Create a {@link CompressionInputStream} that will read from the given
     * input stream.
     *
     * @param in the stream to read compressed bytes from
     * @return a stream to read uncompressed bytes from
     * @throws IOException
     */
    @Override
    public CompressionInputStream createInputStream(InputStream in)
            throws IOException {
        return CompressionCodec.Util.
                createInputStreamWithCodecPool(this, conf, in);
    }

    /**
     * Create a {@link CompressionInputStream} that will read from the given
     * {@link InputStream} with the given {@link Decompressor}.
     *
     * @param in           the stream to read compressed bytes from
     * @param decompressor decompressor to use
     * @return a stream to read uncompressed bytes from
     * @throws IOException
     */
    @Override
    public CompressionInputStream createInputStream(InputStream in,
                                                    Decompressor decompressor)
            throws IOException {
        checkNativeCodeLoaded();
        return new BlockDecompressorStream(in, decompressor, conf.getInt(
                CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFERSIZE_KEY,
                CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFERSIZE_DEFAULT));
    }

    /**
     * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
     *
     * @return the type of decompressor needed by this codec.
     */
    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        checkNativeCodeLoaded();
        return ZstdDecompressor.class;
    }

    /**
     * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
     *
     * @return a new decompressor for use by this codec
     */
    @Override
    public Decompressor createDecompressor() {
        checkNativeCodeLoaded();
        int bufferSize = conf.getInt(
                CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFERSIZE_KEY,
                CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFERSIZE_DEFAULT);
        return new ZstdDecompressor(bufferSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DirectDecompressor createDirectDecompressor() {
        return isNativeCodeLoaded() ? new ZstdDirectDecompressor() : null;
    }

    /**
     * Get the default filename extension for this kind of compression.
     *
     * @return <code>.snappy</code>.
     */
    @Override
    public String getDefaultExtension() {
        return ".zst";
    }
}


