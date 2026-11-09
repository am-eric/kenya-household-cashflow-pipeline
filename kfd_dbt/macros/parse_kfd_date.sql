{# KFD dates come in two formats: 29jul2013 and 29-Jul-13. Anything else becomes null. #}
{% macro parse_kfd_date(col) -%}
    case
        when {{ col }} ~ '^[0-9]{1,2}[A-Za-z]{3}[0-9]{4}$'     then to_date({{ col }}, 'DDMonYYYY')
        when {{ col }} ~ '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}$'   then to_date({{ col }}, 'DD-Mon-YY')
    end
{%- endmacro %}
