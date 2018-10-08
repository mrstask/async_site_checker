import sqlalchemy as sa

TABLE_NAME_1 = 'domains_data'
TABLE_NAME_2 = 'meta_ua_catalog'
metadata = sa.MetaData()


connection = {'user': 'stask', 'database': 'myproject', 'host': 'localhost', 'password': 'trololo123'}
dsn = 'postgresql://{user}:{password}@{host}/{database}'.format(**connection)
engine = sa.create_engine(dsn)
metadata.bind = engine

parse_results = sa.Table(
    TABLE_NAME_1, metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('catalog_domain', sa.String),
    sa.Column('response_domain', sa.String),
    sa.Column('response_code', sa.String),
    sa.Column('ssl', sa.Boolean, default=False),
    sa.Column('title', sa.String),
    sa.Column('description', sa.String),
    sa.Column('response_headers', sa.String),
    sa.Column('response_body', sa.String),
    sa.Column('source_id', sa.Integer),
    )
meta_domains = sa.Table(
    TABLE_NAME_2, metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('checked', sa.Boolean, default=False),
    sa.Column('domain', sa.String),
    sa.Column('category', sa.String),
    )

if __name__ == '__main__':
    # metadata.drop_all()
    metadata.create_all()




