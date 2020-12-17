"""
Transfers data from AWS Redshift into a S3 Bucket.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults



class RedshiftToS3Transfer(BaseOperator):
    template_fields = ('s3_key',)
    """
    Executes an UNLOAD command to s3 as a CSV with headers

    :param schema: reference to a specific schema in redshift database
    :type schema: str
    :param table: reference to a specific table in redshift database
    :type table: str
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key. If ``table_as_file_name`` is set
        to False, this param must include the desired file name
    :type s3_key: str
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param unload_options: reference to a list of UNLOAD options
    :type unload_options: list
    :param autocommit: If set to True it will automatically commit the UNLOAD statement.
        Otherwise it will be committed right before the redshift connection gets closed.
    :type autocommit: bool
    :param include_header: If set to True the s3 file contains the header columns.
    :type include_header: bool
    :param table_as_file_name: If set to True, the s3 file will be named as the table
    :param query: If set to string will override default select query
    :type table_as_file_name: bool
    """

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
            self,
            schema,
            table,
            s3_bucket,
            s3_key,
            redshift_conn_id='redshift_default',
            aws_conn_id='aws_default',
            verify=None,
            unload_options=tuple(),
            autocommit=False,
            include_header=False,
            table_as_file_name=True,  # Set to True by default for not breaking current workflows
            query=None,
            *args, **kwargs):
        super(RedshiftToS3Transfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.unload_options = unload_options
        self.autocommit = autocommit
        self.include_header = include_header
        self.query = query
        self.table_as_file_name = table_as_file_name

        if self.include_header and 'HEADER' not in [uo.upper().strip() for uo in self.unload_options]:
            self.unload_options = list(self.unload_options) + ['HEADER', ]


    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.log.info("S3 Key: %s", self.s3_key)

        credentials = s3_hook.get_credentials()
        unload_options = '\n\t\t\t'.join(self.unload_options)
        s3_key = '{}/{}_'.format(self.s3_key, self.table) if self.table_as_file_name else self.s3_key

        select_query = "SELECT * FROM {schema}.{table}".format(schema=self.schema, table=self.table) if self.query is None else self.query
        unload_query = """
                    UNLOAD ('{select_query}')
                    TO 's3://{s3_bucket}/{s3_key}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {unload_options};
                    """.format(select_query=select_query,
                               s3_bucket=self.s3_bucket,
                               s3_key=s3_key,
                               access_key=credentials.access_key,
                               secret_key=credentials.secret_key,
                               unload_options=unload_options)

        self.log.info('Executing UNLOAD command...')
        postgres_hook.run(unload_query, self.autocommit)
        self.log.info("UNLOAD command complete...")

        return s3_key
