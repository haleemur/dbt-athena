
-- - get_catalog
-- - list_relations_without_caching
-- - get_columns_in_relation

{% macro presto_ilike(column, value) -%}
	regexp_like({{ column }}, '(?i)\A{{ value }}\Z')
{%- endmacro %}


{% macro athena__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          case when regexp_like(data_type, 'varchar\(\d+\)') then 'varchar'
               else data_type
          end as data_type,
          case when regexp_like(data_type, 'varchar\(\d+\)') then
                  from_base(regexp_extract(data_type, 'varchar\((\d+)\)', 1), 10)
               else NULL
          end as character_maximum_length,
          NULL as numeric_precision,
          NULL as numeric_scale

      from
      {{ relation.information_schema('columns') }}

      where {{ presto_ilike('table_name', relation.identifier) }}
        {% if relation.schema %}
        and {{ presto_ilike('table_schema', relation.schema) }}
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro athena__list_relations_without_caching(information_schema, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as database,
      table_name as name,
      table_schema as schema,
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type
    from {{ information_schema }}.tables
    where {{ presto_ilike('table_schema', schema) }}
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro athena__reset_csv_table(model, full_refresh, old_relation) %}
    {{ adapter.drop_relation(old_relation) }}
    {{ return(create_csv_table(model)) }}
{% endmacro %}


{% macro athena__format_ctas_options() %}
  {%- set _fmt = config.get('format') -%}
  {%- set _compression = config.get('compression') -%}
  {%- set _partitioned_by = config.get('partitioned_by') -%}
  {%- set _bucketed_by = config.get('bucketed_by') -%}
  {%- set _bucket_count = config.get('bucket_count') -%}
  {%- set _external_location = config.get('external_location') -%}
  {%- set opts = '' -%}
  {%- if _fmt -%}
    {%- set opts = " format='" + _fmt + "'" -%}
  {%- endif -%}
  {%- if _compression -%}
    {%- if _format == 'orc'-%}
      {%- set opts = opts + ", orc_compression='" + _compression + "'" -%}
    {%- elif _format == 'parquet' -%}
      {%- set opts = opts + ", parquet_compression='" + _compression + "'" -%}
    {%- endif -%}
  {%- endif -%}
  {%- if _partitioned_by -%}
    {%- if opts != '' -%}{%- set opts = opts + ',' -%}{%- endif -%}
    {% set cols = '' %}
    {%- set opts = opts + " partitioned_by=ARRAY" + _partitioned_by | tojson | replace('"', "'") -%}
  {%- endif -%}
  {%- if _bucketed_by -%}
    {%- if opts != '' -%}{%- set opts = opts + ',' -%}{%- endif -%}
    {%- set cols = '' -%}
    {%- set opts = opts + " bucketed_by=ARRAY" + _bucketed_by | tojson | replace('"', "'") -%}
  {%- endif -%}
  {%- if _bucket_count -%}
    {%- if opts != ''%}{% set opts = opts + ',' -%}{%- endif -%}
    {%- set opts = opts + " bucket_count=" + _bucket_count -%}
  {%- endif -%}
  {%- if _external_location -%}
    {%- if opts != ''%}{% set opts = opts + ',' -%}{%- endif -%}
    {%- set opts = opts + " external_location='" + _external_location + "'" -%}
  {%- endif -%}
  {%- if opts -%}WITH ({{ opts }}){%- endif -%} 
{% endmacro %}

{% macro athena__create_table_as(temporary, relation, sql) -%}
  create table
    {{ relation }} {{ athena__format_ctas_options() }}
  as (
    -- wrapping to select allows to use "with" statements inside "create table"
    select * from (
        {{ sql }}
    )
  );
{% endmacro %}


{% macro athena__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}


{% macro athena__drop_schema(database_name, schema_name) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{schema_name}}
  {% endcall %}
{% endmacro %}
{% macro athena__create_schema(database_name, schema_name) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{schema_name}}
  {% endcall %}
{% endmacro %}


{% macro athena__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
     alter {{ from_relation.type }} {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}


{% macro athena__load_csv_rows(model) %}
  {{ return(basic_load_csv_rows(model, 1000)) }}
{% endmacro %}


{% macro athena__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.schemata
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro athena__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
        select count(*)
        from {{ information_schema }}.schemata
        where {{ presto_ilike('catalog_name', information_schema.database) }}
          and {{ presto_ilike('schema_name', schema) }}
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}
