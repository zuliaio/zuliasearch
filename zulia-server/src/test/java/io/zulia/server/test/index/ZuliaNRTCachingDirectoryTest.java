package io.zulia.server.test.index;

import io.zulia.server.index.ZuliaNRTCachingDirectory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Behavioral tests for ZuliaNRTCachingDirectory: it caches new segments when enabled, caches nothing when disabled
 * or when the cache budget is zero, and reads its thresholds live (so a disable takes effect without reopening the
 * IndexWriter). A flush via DirectoryReader.open(writer) exercises the real NRT write path; commit drains the cache.
 */
public class ZuliaNRTCachingDirectoryTest {

	private static IndexWriter writer(Directory dir) throws Exception {
		return new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()));
	}

	private static void addDoc(IndexWriter writer, String id) throws Exception {
		Document doc = new Document();
		doc.add(new StringField("id", id, Field.Store.YES));
		writer.addDocument(doc);
	}

	@Test
	public void cachesNewSegmentWhenEnabled() throws Exception {
		Directory delegate = new ByteBuffersDirectory();
		try (ZuliaNRTCachingDirectory dir = new ZuliaNRTCachingDirectory(delegate, () -> 50, () -> 150, () -> false); IndexWriter writer = writer(dir)) {
			addDoc(writer, "1");
			// Opening an NRT reader flushes a segment through the directory; with caching enabled it lands in RAM.
			try (DirectoryReader reader = DirectoryReader.open(writer)) {
				Assertions.assertEquals(1, reader.numDocs());
			}
			Assertions.assertTrue(dir.ramBytesUsed() > 0, "new segment should be cached in RAM when caching is enabled");
		}
	}

	@Test
	public void cachesNothingWhenDisabled() throws Exception {
		Directory delegate = new ByteBuffersDirectory();
		try (ZuliaNRTCachingDirectory dir = new ZuliaNRTCachingDirectory(delegate, () -> 50, () -> 150, () -> true); IndexWriter writer = writer(dir)) {
			addDoc(writer, "1");
			try (DirectoryReader reader = DirectoryReader.open(writer)) {
				Assertions.assertEquals(1, reader.numDocs(), "search-after-write must still work with caching disabled");
			}
			Assertions.assertEquals(0, dir.ramBytesUsed(), "nothing should be cached in RAM when caching is disabled");
		}
	}

	@Test
	public void cachesNothingWhenBudgetIsZero() throws Exception {
		Directory delegate = new ByteBuffersDirectory();
		// maxCachedMB == 0 -> a zero-byte budget gates out all caching even though caching is enabled.
		try (ZuliaNRTCachingDirectory dir = new ZuliaNRTCachingDirectory(delegate, () -> 50, () -> 0, () -> false); IndexWriter writer = writer(dir)) {
			addDoc(writer, "1");
			try (DirectoryReader reader = DirectoryReader.open(writer)) {
				Assertions.assertEquals(1, reader.numDocs());
			}
			Assertions.assertEquals(0, dir.ramBytesUsed(), "a zero cache budget should cache nothing");
		}
	}

	@Test
	public void disableTakesEffectLiveWithoutReopen() throws Exception {
		AtomicBoolean disabled = new AtomicBoolean(false);
		Directory delegate = new ByteBuffersDirectory();
		try (ZuliaNRTCachingDirectory dir = new ZuliaNRTCachingDirectory(delegate, () -> 50, () -> 150, disabled::get); IndexWriter writer = writer(dir)) {
			addDoc(writer, "1");
			try (DirectoryReader reader = DirectoryReader.open(writer)) {
				Assertions.assertEquals(1, reader.numDocs());
			}
			Assertions.assertTrue(dir.ramBytesUsed() > 0, "enabled: first segment is cached");

			// commit syncs, which drains the RAM cache to the delegate
			writer.commit();
			Assertions.assertEquals(0, dir.ramBytesUsed(), "commit should drain the NRT cache");

			// flip the live flag - no writer or directory reopen
			disabled.set(true);
			addDoc(writer, "2");
			try (DirectoryReader reader = DirectoryReader.open(writer)) {
				Assertions.assertEquals(2, reader.numDocs(), "writes still succeed after a live disable");
			}
			Assertions.assertEquals(0, dir.ramBytesUsed(), "after live disable, new segments must not be cached");
		}
	}

	@Test
	public void toStringReflectsLiveState() throws Exception {
		AtomicBoolean disabled = new AtomicBoolean(false);
		try (ZuliaNRTCachingDirectory dir = new ZuliaNRTCachingDirectory(new ByteBuffersDirectory(), () -> 50, () -> 150, disabled::get)) {
			Assertions.assertTrue(dir.toString().contains("disabled=false"), "toString should show the current disabled flag");
			disabled.set(true);
			Assertions.assertTrue(dir.toString().contains("disabled=true"), "toString must reflect the live disabled flag");
		}
	}
}
