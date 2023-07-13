from constructs import Construct

from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_s3 as s3,
    aws_glue as glue,
    RemovalPolicy as Removal_Policy,
    aws_s3_assets as s3_assets,
    aws_s3_deployment as s3deploy,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_secretsmanager as secretsmanager,
    
)


class CrukCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bronze_bucket = s3.Bucket(self, "BronzeBucket",
            bucket_name="qa-nyc-bronze", 
            versioned=True,
            removal_policy=Removal_Policy.DESTROY,  # This line allows CDK to delete the bucket when the stack is deleted
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            auto_delete_objects=True
        )

        # Create the 'qa-nyc-silver' bucket
        silver_bucket = s3.Bucket(self, "SilverBucket",
            bucket_name="qa-nyc-silver",
            removal_policy=Removal_Policy.DESTROY,  # This line allows CDK to delete the bucket when the stack is deleted            
            versioned=True,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            auto_delete_objects=True
        )

        # Create the 'qa-nyc-gold' bucket
        gold_bucket = s3.Bucket(self, "GoldBucket",
            bucket_name="qa-nyc-gold",
            removal_policy=Removal_Policy.DESTROY,  # This line allows CDK to delete the bucket when the stack is deleted            
            versioned=True,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            auto_delete_objects=True
        )

        # Create the 'qa-placeholder' bucket
        placeholder_bucket = s3.Bucket(self, "PlaceholderBucket",
            bucket_name="qa-placeholder",
            removal_policy=Removal_Policy.DESTROY,  # This line allows CDK to delete the bucket when the stack is deleted
            versioned=True,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            auto_delete_objects=True,
        )
        # Create an asset for the local file
        asset = s3_assets.Asset(self, "Asset", path="./python-scripts")

        # Create a bucket deployment to upload the local file to the bucket
        s3deploy.BucketDeployment(self, "DeployAsset",
            sources=[s3deploy.Source.asset("./python-scripts")],
            destination_bucket=gold_bucket,
        )

        # Create an IAM role with full administration access
        role = iam.Role(self, "MyRole",
            role_name="CDK-Admin-Role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("rds.amazonaws.com"),
                iam.ServicePrincipal("s3.amazonaws.com"),
                iam.ServicePrincipal("glue.amazonaws.com"),
                iam.ServicePrincipal("athena.amazonaws.com"), 
                iam.ServicePrincipal("transfer.amazonaws.com"),  
            ),
        )
        role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"))


        # Create a VPC with default configuration
        vpc = ec2.Vpc(self, "VPC")

        scripts = [
            "ETL_Job_1_Rawdata_to_S3.py",
            "ETL_Job_2_Cleaning.py",
            "ETL_Job_3_Final_Transformation.py",
            "ETL_Job_4_Load_to_DB.py",
        ]

        buckets = {
            "ETL_Job_1_Rawdata_to_S3.py": {"name": bronze_bucket, "input": 'qa-nyc-bronze', "output": '2021/march/yellow_tripdata_2021-03.parquet'},
            "ETL_Job_2_Cleaning.py": {"name": silver_bucket, "input": 's3://qa-nyc-bronze/2021/march/yellow_tripdata_2021-03.parquet', "output": 's3://qa-nyc-silverr/2021/march/curated_data'},
            "ETL_Job_3_Final_Transformation.py": {"name": gold_bucket, "input": 's3://qa-nyc-silverr/2021/march/curated_data"', "output": 's3://qa-nyc-gold/2021/march/enriched_data'},
            "ETL_Job_4_Load_to_DB.py": {"name": placeholder_bucket, "input": 's3://nyc-gold/2021/march/enriched_data/', "output": ''},
        }

        glue_job_names = []
        glueJobs = []
        for script in scripts:
            # Create an asset for the script
            # asset = s3_assets.Asset(self, f"Asset{script}", path=f"python-scripts/{script}")

            # Get the input and output buckets for this job
            input_bucket = buckets[script]["input"]
            output_bucket = buckets[script]["output"]
            bucketName = buckets[script]["name"]
            script_loc = f"s3://qa-placeholder/python-scripts/{script}"

            # Upload the script to the input bucket
            # s3deploy.BucketDeployment(self, f"Deploy_{script}",
            #     sources=[s3deploy.Source.asset(f"./python-scripts/{script}")],
            #     destination_bucket=bucketName,
            # )

            # Create the Glue job
            glueJob = glue.CfnJob(self, f"jobs_{script.replace('.py', '')}",
                role=role.role_arn,
                command=glue.CfnJob.JobCommandProperty(
                    name="glueetl", 
                    python_version="3", 
                    script_location=script_loc  # This will point to the script in the input bucket
                ),
                default_arguments={
                    "--job-language": "python",
                    "--job-bookmark-option": "job-bookmark-enable",
                    "--input_bucket": input_bucket,
                    "--output_bucket": output_bucket,
                },
                glue_version="3.0",
                max_retries=0,
                timeout=10,
                max_capacity=2.0,
                name=f"QA_{script.replace('.py', '')}",
               
                
            )
            glueJobs.append(glueJob)
            glue_job_names.append(script.replace('.py', ''))
        
        # Bucket names list
        bucket_names = ["qa-nyc-bronze", "qa-nyc-silver", "qa-nyc-gold"]
        # buckets = [s3.Bucket(self, bucket_name, bucket_name=bucket_name) for bucket_name in bucket_names]

        # Create the Glue database
        database = glue.CfnDatabase(self, "Database", 
            catalog_id=self.account,  # The ID of the data catalog in which to create the database
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="nyc-cruk-db",
                description="Database for NYC CRUK project",
            )
        )

        s3Paths = ["s3://qa-nyc-bronze/2021/march/yellow_tripdata_2021-03.parquet", "s3://qa-nyc-silver/2021/march/curated_data", "s3://qa-nyc-gold/2021/march/enriched_data"]
        
        # Create a Glue crawler for each bucket
        glue_crawler_names = []
        glueCrawlers = []
        for i, bucket in enumerate(bucket_names):
            glueCrawler = glue.CfnCrawler(self, f"crawler_{scripts[i].replace('.py', '')}",
                name=f"QA_{scripts[i].replace('.py', '')}",
                role=role.role_arn,  # Replace with the ARN of your Glue IAM role
                database_name=database.database_input.name,
                targets=glue.CfnCrawler.TargetsProperty(
                    s3_targets=[glue.CfnCrawler.S3TargetProperty(
                        path=s3Paths[i]
                    )]
                ),
                schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                    delete_behavior="LOG",
                    update_behavior="UPDATE_IN_DATABASE"
                ),
                table_prefix=f"{bucket}_",

            )
            glue_crawler_names.append(scripts[i].replace('.py', ''))
            glueCrawlers.append(glueCrawler)

        # Create a Glue connection
        connection = glue.CfnConnection(self, "MyConnection",
            catalog_id=self.account,  # The ID of the data catalog in which to create the connection
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                connection_type="JDBC",
                name="QA_ETL_Postgres",
                connection_properties={
                    "JDBC_CONNECTION_URL": "jdbc:postgresql://database-nyc.cluster-cygmkiizijyp.eu-west-2.rds.amazonaws.com:5432/nyccrukdb",
                    "USERNAME": "nycnur",
                    "PASSWORD": "nyc12345"
                },
                description="JDBC connection to my RDS instance",
                match_criteria=["string"],
            )
        )
        # Create a Glue crawler with a JDBC target
        glueCrawler = glue.CfnCrawler(self, "crawler_S3toPostgres",
            name="QA_S3toPostgres",
            role=role.role_arn,  # Replace with the ARN of your Glue IAM role
            database_name=database.database_input.name,
            targets=glue.CfnCrawler.TargetsProperty(
                jdbc_targets=[glue.CfnCrawler.JdbcTargetProperty(
                    connection_name=connection.connection_input.name,
                    path="nyccrukdb/%"
                )]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG",
                update_behavior="UPDATE_IN_DATABASE"
            ),
        )
        glue_crawler_names.append("S3toPostgres")
        glueCrawlers.append(glueCrawler)

        # Create a secret to hold the master username and password
        master_user_secret = secretsmanager.Secret(self, "MasterUserSecret",
            description="Credentials for the master user of the Aurora Serverless DB cluster",
            secret_name="AuroraServerlessMasterUserSecret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username": "nycnur"}',  # change here
                generate_string_key="password",  # and here
                exclude_characters='{}[]()/\'"\\`_@:;<>+=|^?*~%',
                password_length=30,
            ),
        )


        # Create an Aurora Serverless v1 DB cluster
        serverless_cluster = rds.ServerlessCluster(self, "ServerlessCluster",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_13_9  # Replace with your desired version
            ), # Use VER_13_9 if your CDK version supports it
            credentials=rds.Credentials.from_secret(master_user_secret),
            
            vpc=vpc,  # Replace with your VPC
            scaling=rds.ServerlessScalingOptions(
                auto_pause= Duration.minutes(10),  # Pause after 10 minutes of inactivity
                min_capacity=rds.AuroraCapacityUnit.ACU_2,  # Minimum 2 ACUs
                max_capacity=rds.AuroraCapacityUnit.ACU_8,  # Maximum 8 ACUs
            ),
            default_database_name="nyccrukdb",
            removal_policy= Removal_Policy.DESTROY,  # NOT recommended for production environments
            cluster_identifier="qa-database-nyc",
            enable_data_api=True,
        )

        # Create a policy that allows connecting to the RDS instance
        connect_policy = iam.PolicyStatement(
            actions=["rds-db:connect"],
            resources=[f"{serverless_cluster.cluster_arn}/*"],
        )

        # Attach the policy to the role
        role.add_to_policy(connect_policy)

        # Create a Glue workflow
        workflow = glue.CfnWorkflow(self, "MyWorkflow", name="QA_WF_ETL_NYC_CRUK")


        # Create the first trigger which is activated when the workflow starts
        tr_rawdata_to_s3 = glue.CfnTrigger(self, "Trigger1",
            actions=[glue.CfnTrigger.ActionProperty(job_name=glueJobs[0].name)],
            type="ON_DEMAND",
            workflow_name=workflow.name,
        )

        # Create the second trigger which is activated when the first job completes
        tr_cleaning = glue.CfnTrigger(self, "Trigger2",
            actions=[
                glue.CfnTrigger.ActionProperty(crawler_name=glueCrawlers[0].name),
                glue.CfnTrigger.ActionProperty(job_name=glueJobs[1].name)
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=glueJobs[0].name,
                    logical_operator="EQUALS",
                    state="SUCCEEDED"
                )],
                logical="ANY"
            ),
            type="CONDITIONAL",
            start_on_creation=False,
            workflow_name=workflow.name,
        )

        # Add a dependency to the second trigger on the first trigger
        tr_cleaning.add_depends_on(tr_rawdata_to_s3)

        # Create the third trigger which is activated when the second job completes
        tr_final_tranform = glue.CfnTrigger(self, "Trigger3",
            actions=[
                glue.CfnTrigger.ActionProperty(crawler_name=glueCrawlers[1].name),
                glue.CfnTrigger.ActionProperty(job_name=glueJobs[2].name)
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=glueJobs[1].name,
                    logical_operator="EQUALS",
                    state="SUCCEEDED"
                )],
                logical="ANY"
            ),
            type="CONDITIONAL",
            start_on_creation=False,
            workflow_name=workflow.name,
        )

        # Add a dependency to the third trigger on the second trigger
        tr_final_tranform.add_depends_on(tr_cleaning)

        # Create the fourth trigger which is activated when the third job completes
        tr_last_crawlers = glue.CfnTrigger(self, "Trigger4",
            actions=[
                glue.CfnTrigger.ActionProperty(crawler_name=glueCrawlers[2].name),
                glue.CfnTrigger.ActionProperty(crawler_name=glueCrawlers[3].name),
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=glueJobs[2].name,
                    logical_operator="EQUALS",
                    state="SUCCEEDED"
                )],
                logical="ANY"
            ),
            type="CONDITIONAL",
            start_on_creation=False,
            workflow_name=workflow.name,
        )

        # Add a dependency to the fourth trigger on the third trigger
        tr_last_crawlers.add_depends_on(tr_final_tranform)

        # Create the last trigger which is activated when the fourth job completes
        final_trigger = glue.CfnTrigger(self, "FinalTrigger",
            actions=[
                glue.CfnTrigger.ActionProperty(job_name=glueJobs[3].name)  # This is the last job
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=glueJobs[2].name,  # The last trigger waits for the fourth job to complete
                    logical_operator="EQUALS",
                    state="SUCCEEDED"
                )],
                logical="ANY"
            ),
            type="CONDITIONAL",
            start_on_creation=False,
            workflow_name=workflow.name,
        )

        # Add a dependency to the final trigger on the fourth trigger
        final_trigger.add_depends_on(tr_last_crawlers)

       

        # # Create the triggers
        # triggers = []
        # for i in range(4):
        #     # The first trigger is activated when the workflow starts
        #     if i == 0:
        #         trigger = glue.CfnTrigger(self, f"Trigger{i}",
        #             actions=[glue.CfnTrigger.ActionProperty(job_name=glue_job_names[i])],
        #             type="ON_DEMAND",
        #             workflow_name=workflow.name,
        #         )
        #     # The other triggers are activated when the previous job completes
        #     else:
        #         trigger = glue.CfnTrigger(self, f"Trigger{i}",
        #             actions=[
        #                 glue.CfnTrigger.ActionProperty(crawler_name=glue_crawler_names[i-1]), 
        #                 glue.CfnTrigger.ActionProperty(job_name=glue_job_names[i])
        #             ],
        #             predicate=glue.CfnTrigger.PredicateProperty(
        #                 conditions=[glue.CfnTrigger.ConditionProperty(
        #                     job_name=glue_job_names[i-1],
        #                     logical_operator="EQUALS",
        #                     state="SUCCEEDED"
        #                 )],
        #                 logical="ANY"
        #             ),
        #             type="CONDITIONAL",
        #             start_on_creation=False,
        #             workflow_name=workflow.name,
        #         )
        #         # Add dependencies
        #         trigger.node.add_dependency(glueJobs[i-1])
        #         if i > 0:
        #             trigger.node.add_dependency(glueCrawlers[i - 1])

        # # Add a final trigger for the last crawler after the last job completes
        # final_trigger = glue.CfnTrigger(self, "FinalTrigger",
        #     actions=[glue.CfnTrigger.ActionProperty(crawler_name=glue_crawler_names[-1])],
        #     predicate=glue.CfnTrigger.PredicateProperty(
        #         conditions=[glue.CfnTrigger.ConditionProperty(
        #             job_name=glue_job_names[-1],
        #             logical_operator="EQUALS",
        #             state="SUCCEEDED"
        #         )],
        #         logical="ANY"
        #     ),
        #     type="CONDITIONAL",
        #     start_on_creation=False,
        #     workflow_name=workflow.name,
        # )
        # # Add dependencies
        # final_trigger.node.add_dependency(glueJobs[-1])
