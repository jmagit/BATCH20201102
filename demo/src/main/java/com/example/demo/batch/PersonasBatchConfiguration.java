package com.example.demo.batch;

import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.example.demo.model.Persona;
import com.example.demo.model.PersonaCortoDTO;
import com.example.demo.model.PersonaDTO;

@Configuration
@EnableBatchProcessing
public class PersonasBatchConfiguration {
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	// CSV a DB
	public FlatFileItemReader<PersonaDTO> personaCSVItemReader(String fname) {
		return new FlatFileItemReaderBuilder<PersonaDTO>().name("personaCSVItemReader")
				.resource(new ClassPathResource(fname))
				.linesToSkip(1)
				.delimited()
				.names(new String[] { "id", "nombre", "apellidos", "correo", "sexo", "ip" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<PersonaDTO>() { {
						setTargetType(PersonaDTO.class);
					}})
				.build();
	}
	public FlatFileItemReader<PersonaDTO> personaCSV2ItemReader(String fname) {
		return new FlatFileItemReaderBuilder<PersonaDTO>().name("personaCSV2ItemReader")
				.resource(new ClassPathResource(fname))
				.linesToSkip(1)
				.delimited()
				.names(new String[] { "id", "correo", "nombre", "apellidos", "sexo", "ip" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<PersonaDTO>() { {
						setTargetType(PersonaDTO.class);
					}})
				.build();
	}

	@Autowired
	public PersonaItemProcessor personaItemProcessor;

	@Bean
	public JdbcBatchItemWriter<Persona> personaDBItemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Persona>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO personas VALUES (:id,:nombre,:correo,:ip)")
				.dataSource(dataSource)
				.build();
	}

	@Bean
	public Step importCSV2DBStep1(JdbcBatchItemWriter<Persona> personaDBItemWriter) {
		return stepBuilderFactory.get("importCSV2DBStep1")
				.<PersonaDTO, Persona>chunk(10)
				.reader(personaCSVItemReader("personas-1.csv"))
				.processor(personaItemProcessor)
				.writer(personaDBItemWriter)
				.build();
	}

	@Bean
	public Step importCSV2DBStep2(JdbcBatchItemWriter<Persona> personaDBItemWriter) {
		return stepBuilderFactory.get("importCSV2DBStep2")
				.<PersonaDTO, Persona>chunk(10)
				.reader(personaCSVItemReader("personas-2.csv"))
				.processor(personaItemProcessor)
				.writer(personaDBItemWriter)
				.build();
	}
	@Bean
	public Step importCSV2DBStep3(JdbcBatchItemWriter<Persona> personaDBItemWriter) {
		return stepBuilderFactory.get("importCSV2DBStep3")
				.<PersonaDTO, Persona>chunk(10)
				.reader(personaCSV2ItemReader("personas-3.csv"))
				.processor(personaItemProcessor)
				.writer(personaDBItemWriter)
				.build();
	}
	
	// DB a CSV
	@Bean
	JdbcCursorItemReader<Persona> personaDBItemReader(DataSource dataSource) {
		return new JdbcCursorItemReaderBuilder<Persona>().name("personaDBItemReader")
				.sql("SELECT id, nombre, correo, ip FROM personas").dataSource(dataSource)
				.rowMapper(new BeanPropertyRowMapper<>(Persona.class))
				.build();
	}

	public FlatFileItemWriter<Persona> personaCSVItemWriter() {
		return new FlatFileItemWriterBuilder<Persona>().name("personaCSVItemWriter")
				.resource(new FileSystemResource("output/outputData.csv"))
				.lineAggregator(new DelimitedLineAggregator<Persona>() {
					{
						setDelimiter(",");
						setFieldExtractor(new BeanWrapperFieldExtractor<Persona>() {
							{
								setNames(new String[] { "id", "nombre", "correo", "ip" });
							}
						});
					}
				}).build();
	}
	@Bean
	public Step exportDB2CSVStep(JdbcCursorItemReader<Persona> personaDBItemReader) {
		return stepBuilderFactory.get("exportDB2CSVStep")
				.<Persona, Persona>chunk(100)
				.reader(personaDBItemReader)
				.writer(personaCSVItemWriter())
				.build();
	}

	@Bean
	JdbcCursorItemReader<PersonaCortoDTO> personaDBItemReader2(DataSource dataSource) {
		return new JdbcCursorItemReaderBuilder<PersonaCortoDTO>().name("personaDBItemReader")
				.sql("SELECT id, nombre FROM personas").dataSource(dataSource)
				.rowMapper(new BeanPropertyRowMapper<>(PersonaCortoDTO.class))
				.build();
	}

	public FlatFileItemWriter<PersonaCortoDTO> personaCSVItemWriter2() {
		return new FlatFileItemWriterBuilder<PersonaCortoDTO>().name("personaCSVItemWriter")
				.resource(new FileSystemResource("output/outputData2.csv"))
				.lineAggregator(new DelimitedLineAggregator<PersonaCortoDTO>() {
					{
						setDelimiter(",");
						setFieldExtractor(new BeanWrapperFieldExtractor<PersonaCortoDTO>() {
							{
								setNames(new String[] { "id", "nombre" });
							}
						});
					}
				}).build();
	}

	@Bean
	public Step exportDB2CSVStep2(JdbcCursorItemReader<PersonaCortoDTO> personaDBItemReader2) {
		return stepBuilderFactory.get("exportDB2CSVStep2")
				.<PersonaCortoDTO, PersonaCortoDTO>chunk(100)
				.reader(personaDBItemReader2)
				.writer(personaCSVItemWriter2())
				.build();
	}

	// Trabajo
	@Bean
	public Job personasJob(PersonasJobListener listener, Step importCSV2DBStep1, Step importCSV2DBStep2, 
			Step importCSV2DBStep3, Step exportDB2CSVStep, Step exportDB2CSVStep2) {
		return jobBuilderFactory
				.get("personasJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(importCSV2DBStep1)
				.next(importCSV2DBStep2)
				.next(importCSV2DBStep3)
				.next(exportDB2CSVStep)
				.next(exportDB2CSVStep2)
				.build();
	}

}
