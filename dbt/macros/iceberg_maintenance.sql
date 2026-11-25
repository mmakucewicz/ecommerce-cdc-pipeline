{#
  Iceberg table maintenance through Trino:
    optimize            - compacts small data files (frequent streaming MERGEs create many)
    expire_snapshots    - drops old snapshots so metadata does not grow forever
    remove_orphan_files - deletes files no longer referenced by any snapshot

  Usage:
    dbt run-operation iceberg_maintenance --args '{table: lakehouse.silver.orders}'
#}
{% macro iceberg_maintenance(table, file_size_threshold='64MB', retention='7d') %}

    {% set statements = [
        "ALTER TABLE " ~ table ~ " EXECUTE optimize(file_size_threshold => '" ~ file_size_threshold ~ "')",
        "ALTER TABLE " ~ table ~ " EXECUTE expire_snapshots(retention_threshold => '" ~ retention ~ "')",
        "ALTER TABLE " ~ table ~ " EXECUTE remove_orphan_files(retention_threshold => '" ~ retention ~ "')"
    ] %}

    {% for sql in statements %}
        {{ log("Running: " ~ sql, info=True) }}
        {% call statement('maintenance', fetch_result=False, auto_begin=False) %}
            {{ sql }}
        {% endcall %}
    {% endfor %}

    {{ log("Maintenance finished for " ~ table, info=True) }}

{% endmacro %}
