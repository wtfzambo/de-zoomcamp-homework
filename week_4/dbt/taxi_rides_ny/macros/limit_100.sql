{%- macro limit_100() -%}
{%- if var('is_test_run', default=True) -%}

limit 100

{%- endif -%}
{%- endmacro -%}