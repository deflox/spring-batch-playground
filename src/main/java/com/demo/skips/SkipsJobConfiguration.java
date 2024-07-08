package com.demo.skips;

import com.demo.common.BillingData;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
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
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.support.JdbcTransactionManager;

import javax.sql.DataSource;

@Configuration
public class SkipsJobConfiguration {

    @Bean
    public Job skipsJob(JobRepository jobRepository,
                        Step skipsStep) {

        return new JobBuilder("skips-job", jobRepository)
                .start(skipsStep)
                .build();

    }

    @Bean
    public Step skipsStep(JobRepository jobRepository,
                          JdbcTransactionManager transactionManager,
                          ItemReader<BillingData> skipsFileReader,
                          ItemWriter<BillingData> skipsTableWriter,
                          SkipListener<BillingData, BillingData> demoSkipListener) {

        return new StepBuilder("skips-step", jobRepository)
                .<BillingData, BillingData>chunk(100, transactionManager)
                .reader(skipsFileReader)
                .writer(skipsTableWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(10)
                .listener(demoSkipListener)
                .build();

    }

    @Bean
    @StepScope
    public DemoSkipListener demoSkipListener(@Value("#{jobParameters['skip.file']}") String skipFile) {

        return new DemoSkipListener(skipFile);

    }

    @Bean
    @StepScope
    public FlatFileItemReader<BillingData> skipsFileReader(@Value("#{jobParameters['input.file']}") String inputFile) {

        return new FlatFileItemReaderBuilder<BillingData>()
                .name("skips-file-reader")
                .resource(new FileSystemResource(inputFile))
                .delimited()
                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration", "smsCount")
                .targetType(BillingData.class)
                .build();

    }

    @Bean
    public JdbcBatchItemWriter<BillingData> skipsTableWriter(DataSource dataSource) {

        String sql = "insert into BILLING_DATA values (:dataYear, :dataMonth, :accountId, :phoneNumber, :dataUsage, :callDuration, :smsCount)";
        return new JdbcBatchItemWriterBuilder<BillingData>()
                .dataSource(dataSource)
                .sql(sql)
                .beanMapped()
                .build();

    }

}
