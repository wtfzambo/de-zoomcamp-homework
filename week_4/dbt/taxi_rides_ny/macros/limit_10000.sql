{%- macro limit_10000() -%}
{%- if var('is_test_run', default=True) == 'true' -%}

limit 10000

{%- endif -%}
{%- endmacro -%}