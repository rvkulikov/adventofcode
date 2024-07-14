--@formatter:off
drop schema if exists aoc_2023_day03_task02 cascade;
create schema aoc_2023_day03_task02;

set session search_path to aoc_2023_day03_task02,public;

create table aoc_2023_day03_task02.input (
  input_row int4 generated always as identity ,
  input_string text
);

\copy aoc_2023_day03_task02.input (input_string) from '2023/day03task02/input.csv' with (format 'text');

with
  c1000 as (
    select
      *
    from
      aoc_2023_day03_task02.input
  )

  ,c2010 as (
    select
      input_row,
      input_string,
      (v_number[1])::int4 as number_value,
      (row_number() over (partition by input_row, v_number)) as n_number_nth
    from
      c1000,
      regexp_matches(input_string, '\d+', 'g') v_number
    order by
      input_row
  )

  ,c2020 as (
    select
      input_row,
      input_string,
      number_value,
      regexp_instr(input_string, format('\y%s\y', number_value), 1, n_number_nth::int) - 1 as n_number_pos
    from
      c2010
  )
  ,c2999 as (
    select
      input_row,
      input_string,
      number_value,
      box2d(st_geomfromewkt(format('LINESTRING(%s %s, %s %s)',
        n_number_pos, -input_row,
        n_number_pos + length(number_value::text), -input_row + 1
      ))) as number_geom
    from
      c2020
  )



  ,c3010 as (
    select
      input_row,
      input_string,
      v_symbol[1] as symbol_value,
      (row_number() over (partition by input_row, v_symbol)) as n_symbol_nth
    from
      c1000,
      regexp_matches(input_string, '[^\d\.]', 'g') v_symbol
    order by
      input_row
  )

  ,c3020 as (
    select
      input_row,
      input_string,
      symbol_value,
      n_symbol_nth,
      regexp_instr(input_string, format('\%s', symbol_value), 1, n_symbol_nth::int) - 1 as symbol_pos
    from
      c3010
  )

  ,c3999 as (
    select
      input_row,
      input_string,
      symbol_value,
      symbol_pos,
      box2d(st_buffer(st_geomfromewkt(format('LINESTRING(%s %s, %s %s)',
        symbol_pos, -input_row,
        symbol_pos + length(symbol_value::text), -input_row + 1
      )), 1)) as symbol_geom
    from
      c3020
  )

  ,c5010 as (
    select
      input_row,
      input_string,
      symbol_value,
      symbol_geom,

      number_value_array,
      number_value_count,
      number_value_array is not null as numbers_matched
    from
      c3999 s
        left join lateral (
          select
            array_agg(number_value) as number_value_array,
            count(number_value)     as number_value_count
          from
            c2999 number
          where true
            and st_intersects(symbol_geom, number_geom)
            and st_area(st_intersection(symbol_geom, number_geom)) > 0
        ) m on true
    where true
      and symbol_value = '*'
      and number_value_count = 2
  ),
  c5020 as (
    select
      sum(number_value_array[1] * number_value_array[2]) as number_value_sum
    from
      c5010
  )

select * from c5020;
