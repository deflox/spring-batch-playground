package com.demo.partitioning;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class InputFilePartitioner implements Partitioner {

    private final Path inputFolderPath;

    public InputFilePartitioner(String inputFolder) {
        this.inputFolderPath = Path.of(inputFolder);
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();

        try(Stream<Path> fileStream = Files.walk(inputFolderPath)) {
            List<String> fileNames = fileStream
                    .filter(Files::isRegularFile)
                    .map(Path::toString)
                    .toList();

            for (int i = 0; i < fileNames.size(); i++) {
                ExecutionContext executionContext = new ExecutionContext();
                executionContext.put("filePath", fileNames.get(i));
                partitions.put("partition" + (i+1), executionContext);
            }

        } catch (IOException e) {
            throw new RuntimeException("Error reading files: " + e.getMessage());
        }

        return partitions;

    }
}
