from jinja2 import Environment, FileSystemLoader

config_test_ctas_options = {
    'format': 'parquet',
    'compression': 'snappy',
    'partitioned_by': ['col1', 'col2'],
    'bucketed_by': ['col3'],
    'external_location': 's3://a/b/c/'
}

path = 'dbt/include/athena/macros'
fname = 'adapters.sql'

def test_athena__format_ctas_options():

    env = Environment(loader=FileSystemLoader(path))
    env.globals = env.make_globals(d={'config': config_test_ctas_options})
    module = env.get_template(fname).module
    expected = ("WITH ( format='parquet', partitioned_by=ARRAY['col1', 'col2'], "
                "bucketed_by=ARRAY['col3'], external_location='s3://a/b/c/')")
    actual = module.athena__format_ctas_options()
    assert expected == actual

def test_athena__format_ctas_options_blank():
    env = Environment(loader=FileSystemLoader(path))
    env.globals = env.make_globals(d={'config': {}})
    module = env.get_template(fname).module
    expected = ''
    actual = module.athena__format_ctas_options()
    assert expected == actual