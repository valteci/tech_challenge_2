import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script gerado para o node Amazon S3
AmazonS3_node1741819294661 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://valteci-b3-raw"], "recurse": True},
    transformation_ctx="AmazonS3_node1741819294661"
)

# Script gerado para o node Rename Field
RenameField_node1742127304100 = RenameField.apply(
    frame=AmazonS3_node1741819294661,
    old_name="Código",
    new_name="codigo",
    transformation_ctx="RenameField_node1742127304100"
)

# Script gerado para o node Rename Field
RenameField_node1742127320937 = RenameField.apply(
    frame=RenameField_node1742127304100,
    old_name="Ação",
    new_name="empresa",
    transformation_ctx="RenameField_node1742127320937"
)

# Script gerado para o node Rename Field - quantidade
RenameFieldquantidade_node1742127370126 = RenameField.apply(
    frame=RenameField_node1742127320937,
    old_name="`Qtde. Teórica`",
    new_name="quantidade",
    transformation_ctx="RenameFieldquantidade_node1742127370126"
)


# ======================== BLOCO MODIFICADO ========================

# Converte o DynamicFrame para DataFrame
df_raw = RenameFieldquantidade_node1742127370126.toDF()

# Converte a coluna "Data" (string no formato "dd-MM-yyyy") para o tipo date
df_raw = df_raw.withColumn(
    "date_parsed",
    SqlFuncs.to_date(SqlFuncs.col("Data"), "dd-MM-yyyy")
)

# Calcula a data máxima (mais nova) presente no bucket
max_date_row = df_raw.agg(SqlFuncs.max("date_parsed").alias("max_date")).collect()[0]
max_date_val = max_date_row["max_date"]

# Calcula a data anterior (último pregão anterior), se existir
previous_date_row = df_raw.filter(SqlFuncs.col("date_parsed") < max_date_val) \
                          .agg(SqlFuncs.max("date_parsed").alias("prev_date")).collect()
if previous_date_row and previous_date_row[0]["prev_date"] is not None:
    previous_date_val = previous_date_row[0]["prev_date"]
else:
    previous_date_val = None

# --- Dados do pregão atual (data máxima) ---
df_current = df_raw.filter(SqlFuncs.col("date_parsed") == max_date_val)
df_current = df_current.drop("date_parsed")
current_dynamic_frame = DynamicFrame.fromDF(df_current, glueContext, "current_dynamic_frame")

# Agrega os dados do pregão atual agrupando por "empresa", "Data" e "Setor"
aggregate_current = sparkAggregate(
    glueContext,
    parentFrame = current_dynamic_frame,
    groups = ["empresa", "Data", "Setor"],
    aggs = [["quantidade", "sum"]],
    transformation_ctx = "Aggregate_current"
)
df_current_agg = aggregate_current.toDF().withColumnRenamed("sum(quantidade)", "quantidade_total")

# --- Dados do pregão anterior (se existir) ---
if previous_date_val:
    df_previous = df_raw.filter(SqlFuncs.col("date_parsed") == previous_date_val)
    df_previous = df_previous.drop("date_parsed")
    previous_dynamic_frame = DynamicFrame.fromDF(df_previous, glueContext, "previous_dynamic_frame")
    
    # Agrega os dados do pregão anterior agrupando por "empresa" e "Setor"
    aggregate_previous = sparkAggregate(
        glueContext,
        parentFrame = previous_dynamic_frame,
        groups = ["empresa", "Setor"],
        aggs = [["quantidade", "sum"]],
        transformation_ctx = "Aggregate_previous"
    )
    df_previous_agg = aggregate_previous.toDF().withColumnRenamed("sum(quantidade)", "quantidade_total_previous")
    
    # Realiza o join dos dados atuais com os do pregão anterior usando "empresa" e "Setor"
    df_joined = df_current_agg.join(df_previous_agg, on=["empresa", "Setor"], how="left")
    
    # Calcula a diferença entre a quantidade do pregão atual e a do pregão anterior
    df_joined = df_joined.withColumn(
        "diff_quantidade",
        SqlFuncs.when(SqlFuncs.col("quantidade_total_previous").isNull(), SqlFuncs.lit(0))
        .otherwise(SqlFuncs.col("quantidade_total") - SqlFuncs.col("quantidade_total_previous"))
    )
    
    df_final = df_joined
else:
    # Se não houver pregão anterior, define diff_quantidade como 0 para todas as linhas
    df_final = df_current_agg.withColumn("diff_quantidade", SqlFuncs.lit(0))

# Converte o DataFrame final de volta para DynamicFrame
final_dynamic_frame = DynamicFrame.fromDF(df_final, glueContext, "final_dynamic_frame")

# ===================================================================


# Script gerado para o node Aggregate (caso ainda precise de alguma agregação adicional)
RenameField_node1741867697267 = RenameField.apply(
    frame=final_dynamic_frame,
    old_name="`sum(quantidade)`",
    new_name="quantidade_total",
    transformation_ctx="RenameField_node1741867697267"
)

# Script gerado para o node Amazon S3
EvaluateDataQuality().process_rows(
    frame=RenameField_node1741867697267, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741820215329", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)
AmazonS3_node1741820619439 = glueContext.getSink(
    path="s3://valteci-b3-refined", 
    connection_type="s3", 
    updateBehavior="UPDATE_IN_DATABASE", 
    partitionKeys=["Data", "empresa"], 
    enableUpdateCatalog=True, 
    transformation_ctx="AmazonS3_node1741820619439"
)
AmazonS3_node1741820619439.setCatalogInfo(catalogDatabase="default", catalogTableName="bovespa_ETL_glue")
AmazonS3_node1741820619439.setFormat("glueparquet", compression="snappy")
AmazonS3_node1741820619439.writeFrame(RenameField_node1741867697267)
job.commit()
