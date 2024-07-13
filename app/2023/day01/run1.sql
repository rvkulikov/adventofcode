--@formatter:off
drop schema if exists aoc_2023_day01_run1 cascade;
create schema aoc_2023_day01_run1;

set session search_path to aoc_2023_day01_run1,public;

create table aoc_2023_day01_run1.input (
  input_id int4 generated always as identity ,
  input_value text
);

\copy aoc_2023_day01_run1.input (input_value) from '2023/day01/input.csv' with (format 'text');

with
  c1010 as (
    select
      input_value
    from
      input
  ),
  c1020 as (
    select
      input_value,
      input_digits,
      concat_ws('',
        input_digits[array_lower(input_digits, 1)],
        input_digits[array_upper(input_digits, 1)]
      )::int4 as input_checksum
    from
      c1010
        left join lateral (
          select
            array_agg(a[1]) as input_digits
          from
            regexp_matches(input_value, '\d', 'g') t (a)
        ) s on true
  ),
  c1030 as (
    select
      sum(input_checksum) as checksum_sum
    from
      c1020
  )

select * from c1030;
