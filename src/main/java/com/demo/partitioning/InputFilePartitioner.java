package com.demo.partitioning;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputFilePartitioner implements Partitioner {

    private List<String> fileNames;

    public InputFilePartitioner(List<String> fileNames) {
        this.fileNames = fileNames;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>(gridSize);
        for (int i = 0; i < fileNames.size(); i++) {
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.put("fileName", fileNames.get(i));
            partitions.put("partition" + (i+1), executionContext);
        }

        return partitions;

    }
}
