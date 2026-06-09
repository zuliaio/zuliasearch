package io.zulia.data.test;

import io.zulia.data.output.SingleUseDataOutputStream;
import io.zulia.data.target.spreadsheet.excel.ExcelTarget;
import org.apache.poi.util.DefaultTempFileCreationStrategy;
import org.apache.poi.util.TempFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class ExcelTargetTest {

	@Test
	void closeDisposesStreamingTempFiles(@TempDir Path tempDir) throws IOException {
		// route POI's streaming temp files into the test's temp dir (thread-local so other tests are unaffected)
		TempFile.setThreadLocalTempFileCreationStrategy(new DefaultTempFileCreationStrategy(tempDir.toFile()));
		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			SingleUseDataOutputStream outputStream = SingleUseDataOutputStream.from(byteArrayOutputStream, "test.xlsx");

			try (ExcelTarget target = ExcelTarget.withDefaults(outputStream)) {
				// write well past the default in-memory window so rows spill to temp files on disk
				for (int i = 0; i < 2500; i++) {
					target.writeRow("value" + i, i);
				}
			}

			Assertions.assertTrue(byteArrayOutputStream.size() > 0, "expected workbook bytes to be written");

			// POI writes its streaming temp files directly into this dedicated dir, so after close() disposes them
			// nothing should remain
			try (Stream<Path> remaining = Files.list(tempDir)) {
				List<Path> leaked = remaining.toList();
				Assertions.assertTrue(leaked.isEmpty(), "temp dir should be empty after close, but found: " + leaked);
			}
		}
		finally {
			TempFile.setThreadLocalTempFileCreationStrategy(null);
		}
	}

}
