--@formatter:off
drop schema if exists aoc_2023_day01_run2 cascade;
create schema aoc_2023_day01_run2;

set session search_path to aoc_2023_day01_run2,public;

create table aoc_2023_day01_run2.input (
  input_id int4 generated always as identity ,
  input_value text
);

\copy aoc_2023_day01_run2.input (input_value) from '2023/day01/input.csv' with (format 'text');

with
  c1000 as (
    select
      d_parent_digit,
      d_digit,
      d_text as d_pattern,
      d_text || d_digit || d_text as d_replacement
    from
      (values
         (0, 1, 'one'),
         (1, 2, 'two'),
         (2, 3, 'three'),
         (3, 4, 'four'),
         (4, 5, 'five'),
         (5, 6, 'six'),
         (6, 7, 'seven'),
         (7, 8, 'eight'),
         (8, 9, 'nine')
      ) d (d_parent_digit, d_digit, d_text)
  ),

  c1010 as (
    select
      input_id,
      input_value
    from
      aoc_2023_day01_run2.input
  ),
  c1020 as (
    select
      input_id,
      input_value,
      input_normalized
    from
      c1010
        left join lateral (
          with recursive
            c0010 as (
              select
                input_value as value,
                0 as digit

              union

              select
                regexp_replace(value, d.d_pattern, d.d_replacement, 'g') as value,
                d.d_digit as digit
              from
                c0010 p
                  inner join c1000 d on d.d_parent_digit = p.digit
            ),

            c0020 as (
              select
                value as input_normalized
              from
                c0010
              order by
                digit desc
              limit
                1
            )

          select * from c0020
        ) t on true
    )
  ,c1030 as (
    select
      input_id,
      input_value,
      input_normalized,
      input_digits,
      concat_ws('',
        input_digits[array_lower(input_digits, 1)],
        input_digits[array_upper(input_digits, 1)]
      )::text as input_checksum
    from
      c1020
        left join lateral (
          select
            array_agg(a[1]) as input_digits
          from
            regexp_matches(input_normalized, '\d', 'g') t (a)
        ) s on true
  )
  ,c1040 as (
    select
      sum(input_checksum::int4) as checksum_sum
    from
      c1030
  )

select * from c1040;
