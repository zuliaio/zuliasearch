package io.zulia.server.index.cache;

import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * A category must keep one ordinal for the life of the taxonomy, however the writer cache evicts it. The workload is a hot set of
 * categories that recur on every document plus a stream of new ones that recur a little later, the shape of an author facet. Each
 * hot lookup promotes its entry past the newest one in the access ordered cache, so the newest category is the first evicted, and
 * re-adding it before the taxonomy reader has seen it is how a second ordinal appears.
 */
public class ZuliaTaxonomyWriterCacheOrdinalTest {

	private static final int CACHE_SIZE = 64;
	private static final int HOT = 48;
	private static final int NEW_CATEGORIES = 500;
	private static final int RECUR_WITHIN = 8;

	@Test
	public void reAddedCategoryKeepsItsOrdinal() throws Exception {
		try (ByteBuffersDirectory directory = new ByteBuffersDirectory(); DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(directory,
				IndexWriterConfig.OpenMode.CREATE, new ZuliaTaxonomyWriterCache(CACHE_SIZE))) {
			List<FacetLabel> hot = new ArrayList<>();
			for (int i = 0; i < HOT; i++) {
				FacetLabel label = new FacetLabel("author", "hot" + i);
				hot.add(label);
				writer.addCategory(label);
			}
			int duplicates = 0;
			List<FacetLabel> fresh = new ArrayList<>();
			List<Integer> ordinals = new ArrayList<>();
			for (int i = 0; i < NEW_CATEGORIES; i++) {
				FacetLabel label = new FacetLabel("author", "new" + i);
				fresh.add(label);
				ordinals.add(writer.addCategory(label));
				for (FacetLabel hotLabel : hot) {
					writer.addCategory(hotLabel);
				}
				// the recent new categories recur a few documents after they first appeared
				for (int back = 1; back <= RECUR_WITHIN && i - back >= 0; back++) {
					if (writer.addCategory(fresh.get(i - back)) != ordinals.get(i - back)) {
						duplicates++;
					}
				}
			}
			Assertions.assertEquals(0, duplicates, "categories that were assigned a second ordinal out of " + NEW_CATEGORIES);
			Assertions.assertEquals(HOT + NEW_CATEGORIES + 2, writer.getSize(), "taxonomy holds root, the dimension and one ordinal per category");
		}
	}
}
