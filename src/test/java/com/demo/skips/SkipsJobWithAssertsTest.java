package com.demo.skips;

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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@SpringBatchTest
@SpringBootTest
public class SkipsJobWithAssertsTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private Job skipsJob;

    @BeforeEach
    public void setUp() {
        this.jobRepositoryTestUtils.removeJobExecutions();
        JdbcTestUtils.deleteFromTables(this.jdbcTemplate, "BILLING_DATA");
    }

    @Test
    void testJobExecution() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.file", "work-dirs/input/skips/billing.csv")
                .addString("skip.file", "work-dirs/output/skips.txt")
                .toJobParameters();

        this.jobLauncherTestUtils.setJob(skipsJob);
        JobExecution jobExecution = this.jobLauncherTestUtils.launchJob(jobParameters);

        Assertions.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        Assertions.assertEquals(498, JdbcTestUtils.countRowsInTable(jdbcTemplate, "BILLING_DATA"));
        Path skipsFilePath = Paths.get("work-dirs", "output", "skips.txt");
        Assertions.assertTrue(Files.exists(skipsFilePath));
        Assertions.assertEquals(2, Files.lines(skipsFilePath).count());

    }

}
