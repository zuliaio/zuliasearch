
. A store landing in
that window can add a new taxonomy ordinal and have its document
captured by the index commit's flush, so a crash then produces a
committed index referencing an ordinal missing from the committed
taxonomy, the exact state the ordering was meant to prevent.

commit() now uses Lucene's two phase pattern: indexWriter.prepareCommit()
freezes the index commit's document set, taxoWriter.commit() makes the
taxonomy durable, indexWriter.commit() publishes the frozen set. Every
ordinal referenced by a frozen document was created before that document
was added, so the committed taxonomy is always a superset of what the
committed index references, regardless of concurrent stores. The method
is also synchronized now because prepareCommit forbids a second pending
commit, and concurrent forceCommit callers could previously overlap
harmlessly under Lucene's internal lock but would now throw.
