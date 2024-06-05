from dagster import DynamicPartitionsDefinition

release_partition = DynamicPartitionsDefinition(name="release")
