{% macro scd_type_2(
    target_model,
    source_model,
    primary_key,
    order_by,
    load_datetime,
    columns,
    is_deleted=''
) %}
    {# Dynamically creates the sql code to select all the columns #}
    {% set source_columns = [] %}
    {% set target_prev_columns = [] %}
    {% for column in columns %}
        {% do source_columns.append("source." ~ column) %}
        {% do target_prev_columns.append("target_prev." ~ column) %}
    {% endfor %}
    {% set columns = columns | join(', ') %}
    {% set source_columns = source_columns | join(', ') %}
    {% set target_prev_columns = target_prev_columns | join(', ') %}

    select
        *
    from
    (
        {#
        Fetch all the new rows from the rows from the `source_model`. A row is labeld as `new` if
        its `load_datetime` its >= than the max `load_datetime` in the current model
        #}
        with incremental_source as (
            select
                *
            from
                {{ source_model }}
            {# The first run fetch all rows from the `source_model` #}
            {% if is_incremental() %}
                where {{ load_datetime }} > (
                    select
                        coalesce(max({{ load_datetime }}), to_timestamp('1900-01-01 01:00:00.000'))
                    from
                        {{ target_model }}
                )
            {% endif %}
        ),

        {#
        The block below includes the logic to process records incrementally whose primary key has already
        been seen by the system. Therefore, new rows need to be added to the sequence accordingly.

        The overall incremental logic is governed by a primary key, namely the transaction_hk used as a
        unique identifier for a record. It is a surrogate key computed as the SHA1 hash of the `primary_key`
        and the value of the `order_by` column of the record.
        
        This macro expects a model that uses it to be defined as incremental with merge strategy, with the
        primary key being the transaction_hk.
        #}
        {% if is_incremental() %}
        {# 
        This CTE includes all the new rows that need to be inserted into an existing sequence, either
        between current rows or at the end. Each record is related to a single new update, including
        its primary key and all its columns. Additionally, it provides references to the previous row
        in the sequence and the subsequent one, along with their attributes. 
        #}
        updates as (
            select
                {# Attributes of the new record. #}
                source.{{ primary_key }},
                struct({{ source_columns }}) as source_columns,
                struct({{ target_prev_columns }}) as target_prev_columns,
                source.{{ load_datetime }},
                source.{{ order_by }} as source_order_by,
                {#
                If a deletion condition has been specified, the block below adds a column to flag if
                the new record flags a deletion.
                #}
                {% if is_deleted != '' %}
                source.{{is_deleted}} as source_is_deleted,
                {% endif %}

                {# Attributes of the previous row in the sequence. #}
                target_prev.transaction_hk as prev_transaction_hk,
                target_prev.effective_from as prev_effective_from,
                target_prev.effective_to as prev_effective_to,
                target_prev.is_current as prev_is_current,
                target_prev.{{load_datetime}} as prev_load_datetime,

                {# Attributes of the subsequent row in the sequence. #}
                target_next.effective_from as next_effective_to,

                {#
                The two columns below indicate the position of the new record in descending and ascending
                order within the sequence, given by its predecessor and successor.
                #}
                (
                    row_number() over (
                        partition by target_prev.transaction_hk order by source.{{ order_by }} asc
                    )
                ) as scd_row_rank_asc,

                {#
                This column indicates the next value of the `order_by` column within the sequence, given
                by the record's precedessor and succesor.
                #}
                (
                    lead(source.{{ order_by }}) over (
                        partition by target_prev.transaction_hk order by source.{{ order_by }} asc
                    )
                ) as next_order_by
            from
                incremental_source as source
            {#
            Thew new updates are inner joined below with the `target_model` to include all the new rows 
            that need to be inserted into an existing sequence, either between current rows or at the end.
            The join its also used then to find the precedessor record's attributes.
            #}
            inner join
                {{ target_model }} as target_prev
            on
                {#
                This condition checks that the update has a primary key that already exists in the table,
                in other words an existing chain.
                #}
                source.{{ primary_key }} == target_prev.{{ primary_key }}
                and
                {#
                The two conditions below find the predecessor of the update, which is the existing record
                in the sequence that ends before the start of the new record and whose current successor's
                effectiveness starts after the new update.
                #}
                source.{{ order_by }} > target_prev.effective_from
                and
                source.{{order_by}} < target_prev.effective_to
            {#
            The left join below finds the attributes of the successor of the update. This successor is either
            an existing record in the sequence that matches the end of the effectiveness of the predecessor
            found above. If no match is found, all attributes will be null, indicating that the record is at
            the end of the sequence.
            #}
            left join
                {{ target_model }} as target_next
            on
                source.{{ primary_key }} == target_next.{{ primary_key }}
                and
                target_prev.effective_to == target_next.effective_from
        ),
        {#
        This CTE includes updates for the predecessor, modifying its effectiveness to end at the
        start of the effectiveness of the new update. 
        #}
        current_state_update_prev as (
            select
                {{ primary_key }},
                prev_load_datetime as {{ load_datetime }},
                prev_transaction_hk as transaction_hk,
                target_prev_columns.*,
                prev_effective_from as effective_from,
                source_order_by as effective_to,
                {% if is_deleted != '' %}
                source_is_deleted as is_current
                {% else %}
                false as is_current
                {% endif %}
            from
                updates
            where
                scd_row_rank_asc == 1
        ),
        {#
        This CTE includes the new updates that lie between the predecessor and the successor
        in the sequence defined by those two.
        #}
        new_sequence_state as (
            select
                {{ primary_key }},
                {{ load_datetime }},

                upper(sha1(nullif(concat(
                    ifnull(nullif(trim(cast({{ primary_key }} as varchar(20))), ''), 'UNK'), '||',
                    ifnull(nullif(trim(cast(source_order_by as varchar(20))), ''), 'UNK')
                ), 'UNK||UNK'))) as transaction_hk,

                source_columns.*,
                source_order_by as effective_from,

                {#
                The end of the effectiveness of the last record in the sequence is either defined by its successor or,
                if it doesn't exist, is by default set as the maximum date. Additionally, if the successor is not defined,
                the record is marked as the currently valid record in the sequence.
                #}
                coalesce(next_order_by, next_effective_to, to_timestamp('9000-12-31 23:59:59.999')) as effective_to,
                (
                    case when next_order_by is not null then
                        false
                    else
                        next_effective_to is null
                    end
                ) as is_current
            from
                updates
            where
                {% if is_deleted != '' %}
                source_is_deleted is false
                {% endif %}
        ),

        sequence_start_updates as (
            select distinct
                source.*,
                (
                    min(current_sequence.effective_from) over (
                        partition by source.{{ primary_key }}
                    )
                ) as current_sequence_effective_from
            from
                incremental_source as source
            inner join
                {{ target_model }} as current_sequence
            on
                source.{{ primary_key }} == current_sequence.{{ primary_key }}
            left anti join
                updates as sequence_updates
            on
                source.{{ primary_key }} == sequence_updates.{{ primary_key }}
                and
                source.{{ order_by }} = sequence_updates.source_order_by
        ),

        {#
        This CTE orders the starting sequence updates and identifies the next order in
        the sequence for each record.
        #}
        ordered_sequence_start_updates as (
            select
                *,
                (
                    lead({{ order_by }}) over (
                        partition by {{ primary_key }} order by {{ order_by }} asc
                    )
                ) as next_order_by
            from
                sequence_start_updates
        ),

        {#
        This CTE creates the initial state of the new sequence by assigning 
        the effective_from and effective_to dates, as well as generating a
        transaction hash key for each record.
        #}
        new_start_sequence_state as (
            select
                {{ primary_key }},
                {{ load_datetime }},

                upper(sha1(nullif(concat(
                    ifnull(nullif(trim(cast({{ primary_key }} as varchar(20))), ''), 'UNK'), '||',
                    ifnull(nullif(trim(cast({{ order_by }} as varchar(20))), ''), 'UNK')
                ), 'UNK||UNK'))) as transaction_hk,

                {{ columns }},

                {{ order_by }} as effective_from,
                COALESCE(next_order_by, current_sequence_effective_from) as effective_to,
                false as is_current
            from
                ordered_sequence_start_updates
        ),
        {% endif %}

        {# 
        This CTE includes all the new rows that need to be inserted and are not part of an existing
        sequence.
        #}
        new_updates as (
            select
                source.{{ primary_key }},
                source.{{ load_datetime }},

                upper(sha1(nullif(concat(
                    ifnull(nullif(trim(cast(source.{{ primary_key }} as varchar(20))), ''), 'UNK'), '||',
                    ifnull(nullif(trim(cast(source.{{ order_by }} as varchar(20))), ''), 'UNK')
                ), 'UNK||UNK'))) as transaction_hk,

                {{ source_columns }},
                source.{{ order_by }} as effective_from,

                {#
                The column below governs the creation of the chain, where the order is determined by the
                order_by column. It contains the next value of the order_by column in ascending order.
                #}
                (
                    lead(source.{{ order_by }}) over (
                        partition by source.{{ primary_key }} order by source.{{ order_by }} asc
                    )
                ) as candidate_effective_to,

                {#
                The block below creates two additional columns to manage the deletion of the sequence.
                One column flags the row as deleted if the is_deleted property is set to true, and the
                other column flags if the next record is a deletion. This stops the sequence and marks
                the current row accordingly.
                #}
                {% if is_deleted != '' %}
                (
                    lead(source.{{is_deleted}}) over (
                        partition by source.{{ primary_key }} order by source.{{ order_by }} asc
                    )
                ) as next_is_deleted,
                source.{{is_deleted}}
                {% else %}
                false as next_is_deleted
                {% endif %}

            FROM
                incremental_source as source
            {#
            On incremental run the blow below excludes all the rows that belongs to an existing
            sequence.
            #}
            {% if is_incremental() %}
                left anti join
                    {{ target_model }} as target_prev
                on
                    source.{{ primary_key }} == target_prev.{{ primary_key }}
            {% endif %}
        )
        
        {#
        The final result is the union between the updates of existing sequences and the related updates
        for the predecessors, combined with all the new records that do not belong to any existing
        sequence and thus create new sequences in the model.
        #}
        select
            {{ primary_key }},
            {{ load_datetime }},
            transaction_hk,
            {{ columns }},
            effective_from,
            coalesce(candidate_effective_to, to_timestamp('9000-12-31 23:59:59.999')) as effective_to,
            (candidate_effective_to IS NULL OR next_is_deleted) as is_current
        from
            new_updates
        {% if is_deleted != '' %}
        where
            {{is_deleted}} IS FALSE
        {% endif %}
        {% if is_incremental() %}
        union all
        select
            *
        from current_state_update_prev
        union all
        select
            *
        from new_sequence_state
        union all
        select
            *
        from new_start_sequence_state
        {% endif %}   
    )
{% endmacro %}
