package com.demo.partitioning;

import com.demo.common.BillingData;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

@Configuration
public class PartitioningJobConfiguration {

    @Bean
    public Job partitioningJob(JobRepository jobRepository, Step partitioningStep) {

        return new JobBuilder("partitioning-job", jobRepository)
                .start(partitioningStep)
                .build();

    }

    @Bean
    public Step partitioningStep(JobRepository jobRepository,
                                 Step workerStep,
                                 ThreadPoolTaskExecutor threadPoolTaskExecutor,
                                 Partitioner partitioner) {

        return new StepBuilder("partitioning-step", jobRepository)
                .partitioner("worker-step", partitioner)
                .taskExecutor(threadPoolTaskExecutor)
                .step(workerStep)
                .build();

    }

    @Bean
    @StepScope
    public Partitioner partitioner(@Value("#{jobParameters['input.folder']}") String inputFolder) {

        return new InputFilePartitioner(inputFolder);

    }

    @Bean
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(10);
        taskExecutor.setCorePoolSize(5);
        taskExecutor.setQueueCapacity(5);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;

    }

    @Bean
    public Step workerStep(JobRepository jobRepository,
                           JdbcTransactionManager transactionManager,
                           ItemReader<BillingData> partitioningFileReader,
                           ItemWriter<BillingData> partitioningTableWriter) {

        return new StepBuilder("worker-step", jobRepository)
                .<BillingData, BillingData>chunk(10, transactionManager)
                .reader(partitioningFileReader)
                .writer(partitioningTableWriter)
                .build();

    }

    @Bean
    @StepScope
    public FlatFileItemReader<BillingData> partitioningFileReader(@Value("#{stepExecutionContext['filePath']}") String filePath) {

        return new FlatFileItemReaderBuilder<BillingData>()
                .name("partitioning-file-reader")
                .resource(new FileSystemResource(filePath))
                .delimited()
                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration", "smsCount")
                .targetType(BillingData.class)
                .build();

    }

    @Bean
    public JdbcBatchItemWriter<BillingData> partitioningTableWriter(DataSource dataSource) {

        String sql = "insert into BILLING_DATA values (:dataYear, :dataMonth, :accountId, :phoneNumber, :dataUsage, :callDuration, :smsCount)";
        return new JdbcBatchItemWriterBuilder<BillingData>()
                .dataSource(dataSource)
                .sql(sql)
                .beanMapped()
                .build();

    }

}
