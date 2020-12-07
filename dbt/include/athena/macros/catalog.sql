
{% macro athena__get_catalog(information_schema, schemas) -%}
    {%- call statement('catalog', fetch_result=True) -%}
    select * from (

        (
            with tables as (

                select
                    table_catalog as "table_database",
                    table_schema as "table_schema",
                    table_name as "table_name",
                    table_type as "table_type",
                    null as "table_owner"

                from {{ information_schema }}.tables

            ),

            columns as (

                select
                    table_catalog as "table_database",
                    table_schema as "table_schema",
                    table_name as "table_name",
                    null as "table_comment",

                    column_name as "column_name",
                    ordinal_position as "column_index",
                    data_type as "column_type",
                    null as "column_comment"

                from {{ information_schema }}.columns

            )

            select t."table_database"
                 , t."table_schema"
                 , t."table_name"
                 , t."table_type"
                 , t."table_owner"
                 , c."table_comment"
                 , c."column_name"
                 , c."column_index"
                 , c."column_type"
                 , c."column_comment"
            from tables t
            join columns c 
              on t."table_database" = c."table_database"
             and t."table_schema" = c."table_schema" 
             and t."table_name" = t."table_name"
            where t."table_schema" != 'information_schema'
            and (
            {%- for schema in schemas -%}
              upper(t."table_schema") = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
            {%- endfor -%}
            )
            order by "column_index"
        )

    )
  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
