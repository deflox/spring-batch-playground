package com.demo.restartability;

import com.demo.common.BillingData;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.support.JdbcTransactionManager;

import javax.sql.DataSource;

@Configuration
public class RestartabilityJobConfiguration {

    @Bean
    public Job restartabilityJob(JobRepository jobRepository, Step restartabilityStep) {
        return new JobBuilder("restartibility-job", jobRepository)
                .start(restartabilityStep)
                .build();
    }

    @Bean
    public Step restartabilityStep(JobRepository jobRepository,
                                   JdbcTransactionManager transactionManager,
                                   ItemReader<BillingData> billingDataFileReader,
                                   ItemWriter<BillingData> billingDataTableWriter
    ) {
        return new StepBuilder("restartibility-step", jobRepository)
                .<BillingData, BillingData>chunk(100, transactionManager)
                .reader(billingDataFileReader)
                .writer(billingDataTableWriter)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<BillingData> restartabilityFileReader(@Value("#{jobParameters['input.file']}") String inputFile) {
        return new FlatFileItemReaderBuilder<BillingData>()
                .name("restartibility-file-reader")
                .resource(new FileSystemResource(inputFile))
                .delimited()
                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration", "smsCount")
                .targetType(BillingData.class)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<BillingData> restartabilityTableWriter(DataSource dataSource) {
        String sql = "insert into BILLING_DATA values (:dataYear, :dataMonth, :accountId, :phoneNumber, :dataUsage, :callDuration, :smsCount)";
        return new JdbcBatchItemWriterBuilder<BillingData>()
                .dataSource(dataSource)
                .sql(sql)
                .beanMapped()
                .build();
    }

}
