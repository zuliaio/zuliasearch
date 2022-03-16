package io.zulia.server.filestorage.io;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * OutputStream which wraps S3Client, with support for streaming large files directly to S3
 *
 */
public class S3OutputStream extends OutputStream {

	/** Default chunk size is 10MB */
	protected static final int BUFFER_SIZE = 10000000;

	/** S3 client.*/
	private final S3Client s3Client;

	/** The bucket-name on S3 */
	private final String bucket;

	/** The key name within the bucket */
	private final String key;

	/** The temporary buffer used for storing the chunks */
	private final byte[] buf;

	/** The position in the buffer */
	private int position;

	/** The unique id for this upload */
	private String uploadId;

	/** List of parts that have been completed; so I can close the stream*/
	private final List<CompletedPart> completedParts;

	/** indicates whether the stream is still open / valid */
	private boolean open;

	/**
	 * Creates a new S3 OutputStream
	 * @param s3Client the AmazonS3 client
	 * @param bucket name of the bucket
	 * @param key path within the bucket
	 */
	public S3OutputStream(S3Client s3Client, String bucket, String key) {
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.key = key;
		this.buf = new byte[BUFFER_SIZE];
		this.position = 0;
		this.completedParts = new ArrayList<>();
		this.open = true;
	}

	/**
	 * Write an array to the S3 output stream.
	 *
	 * @param b the byte-array to append
	 */
	@Override
	public void write(byte[] b) {
		write(b,0,b.length);
	}

	/**
	 * Writes an array to the S3 Output Stream
	 *
	 * @param byteArray the array to write
	 * @param o the offset into the array
	 * @param l the number of bytes to write
	 */
	@Override
	public void write(final byte[] byteArray, final int o, final int l) {
		this.assertOpen();
		int ofs = o, len = l;
		int size;
		while (len > (size = this.buf.length - position)) {
			System.arraycopy(byteArray, ofs, this.buf, this.position, size);
			this.position += size;
			flushBufferAndRewind();
			ofs += size;
			len -= size;
		}
		System.arraycopy(byteArray, ofs, this.buf, this.position, len);
		this.position += len;
	}

	/**
	 * Flushes the buffer by uploading a part to S3.
	 */
	@Override
	public synchronized void flush() {
		this.assertOpen();
	}

	protected void flushBufferAndRewind() {
		if (uploadId == null) {
			final CreateMultipartUploadRequest cmur = CreateMultipartUploadRequest.builder().bucket(this.bucket).key(this.key).build();
			CreateMultipartUploadResponse resp = s3Client.createMultipartUpload(cmur);
			this.uploadId = resp.uploadId();
		}
		uploadPart();
		this.position = 0;
	}

	protected synchronized void uploadPart() {
		int partNumber = this.completedParts.size() + 1;
		UploadPartRequest upr = UploadPartRequest.builder()
				.bucket(this.bucket)
				.key(this.key)
				.uploadId(this.uploadId)
				.partNumber(partNumber)
				.build();
		UploadPartResponse resp = s3Client.uploadPart(upr, RequestBody.fromInputStream(new ByteArrayInputStream(buf, 0, this.position), this.position));
		completedParts.add(CompletedPart.builder().partNumber(partNumber).eTag(resp.eTag()).build());
	}

	@Override
	public void close() {
		if (this.open) {
			this.open = false;
			if (this.uploadId != null) {
				if (this.position > 0) {
					uploadPart();
				}
				CompletedMultipartUpload cmu = CompletedMultipartUpload.builder().parts(this.completedParts).build();
				CompleteMultipartUploadRequest cmur = CompleteMultipartUploadRequest.builder()
						.bucket(this.bucket)
						.key(this.key)
						.uploadId(this.uploadId)
						.multipartUpload(cmu)
						.build();

				this.s3Client.completeMultipartUpload(cmur);
			}
			else {
				PutObjectRequest req = PutObjectRequest.builder().bucket(bucket).key(key).contentLength((long) this.position).build();
				s3Client.putObject(req, RequestBody.fromInputStream(new ByteArrayInputStream(buf, 0, this.position), this.position));
			}
		}
	}

	public void cancel() {
		this.open = false;
		if (this.uploadId != null) {
			AbortMultipartUploadRequest amur = AbortMultipartUploadRequest.builder()
					.bucket(bucket)
					.key(key)
					.uploadId(uploadId)
					.build();
			this.s3Client.abortMultipartUpload(amur);
		}
	}

	@Override
	public void write(int b) {
		this.assertOpen();
		if (position >= this.buf.length) {
			flushBufferAndRewind();
		}
		this.buf[position++] = (byte)b;
	}

	private void assertOpen() {
		if (!this.open) {
			throw new IllegalStateException("Closed");
		}
	}
}