package com.demo.partitioning;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

@SpringBatchTest
@SpringBootTest
class PartitioningJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private Job partitioningJob;

    @BeforeEach
    public void setUp() {
         this.jobRepositoryTestUtils.removeJobExecutions();
         JdbcTestUtils.deleteFromTables(this.jdbcTemplate, "BILLING_DATA");
    }

    @Test
    void testJobExecution() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .toJobParameters();

        this.jobLauncherTestUtils.setJob(partitioningJob);
        this.jobLauncherTestUtils.launchJob(jobParameters);
    }

}
