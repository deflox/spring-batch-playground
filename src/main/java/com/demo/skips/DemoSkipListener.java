package com.demo.skips;

import com.demo.common.BillingData;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class DemoSkipListener implements SkipListener<BillingData, BillingData> {

    private final Path skippedItemsFile;

    public DemoSkipListener(String skippedItemsFile) {
        this.skippedItemsFile = Paths.get(skippedItemsFile);
    }

    @Override
    public void onSkipInRead(Throwable t) {
        if (t instanceof FlatFileParseException e) {
            String rawLine = e.getInput();
            int lineNumber = e.getLineNumber();
            String skippedLine = lineNumber + "|" + rawLine + System.lineSeparator();
            try {
                Files.writeString(this.skippedItemsFile, skippedLine, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException ex) {
                throw new RuntimeException("Unable to write skipped item " + skippedLine);
            }
        }
    }
}
