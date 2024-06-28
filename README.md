# Macro SCD Type 2 in DBT

## Overview

This repository contains a single file that implements a Slowly Changing Dimension (SCD) Type 2 macro for use with DBT (Data Build Tool). The macro is designed to manage historical changes in dimension tables, ensuring that changes to dimensional attributes are captured over time.

## File Contents

scd_type_2.sql: The SQL file that defines the SCD Type 2 macro.

## Usage

To use the SCD Type 2 macro in your DBT project, follow these steps:

1. **Add the Macro to Your Project:**

   Copy the `scd_type_2.sql` file into the `macros` directory of your DBT project.

2. **Call the Macro:**

   In your DBT model, call the SCD Type 2 macro. Here’s an example of how to use it:

   ```sql
   {{ macros.scd_type_2(
       target_model='your_target_model_name',
       source_model='your_source_model_name',
       primary_key='primary_key_column',
       order_by='order_by_column',
       load_datetime='load_datetime_column',
       columns=['column1', 'column2'],
       is_deleted='optional_is_deleted_column'
   ) }}
 - target_model: The name of the DBT model where historical changes will be tracked.
 - source_model: The name of the DBT model containing the latest data.
 - primary_key: The primary key that uniquely identifies a sequence.
 - order_by: The column(s) used to order records within the sequence.
 - load_datetime: The column in the target table that records when a row is loaded.
 - columns: A list of columns whose changes need to be tracked over time.
 - is_deleted (optional): The column indicating whether a record is a deletion (close the sequence).
