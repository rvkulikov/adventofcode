--@formatter:off
drop schema if exists aoc_2023_day03_run1 cascade;
create schema aoc_2023_day03_run1;

set session search_path to aoc_2023_day03_run1,public;

create table aoc_2023_day03_run1.input (
  input_row int4 generated always as identity ,
  input_value text
);

\copy aoc_2023_day03_run1.input (input_value) from '2023/day03/input.csv' with (format 'text');

with
  c1000 as (
    select
      *
    from
      aoc_2023_day03_run1.input
  )

  ,c2010 as (
    select
      input_row,
      input_value,
      v_number[1] as number_value,
      (row_number() over (partition by input_row, v_number)) as n_number_nth
    from
      c1000,
      regexp_matches(input_value, '\d+', 'g') v_number
    order by
      input_row
  )

  ,c2020 as (
    select
      input_row,
      input_value,
      number_value,
      regexp_instr(input_value, format('\y%s\y', number_value), 1, n_number_nth::int) - 1 as n_number_pos
    from
      c2010
  )
  ,c2999 as (
    select
      input_row,
      input_value,
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
      input_value,
      v_symbol[1] as n_symbol,
      (row_number() over (partition by input_row, v_symbol)) as n_symbol_nth
    from
      c1000,
      regexp_matches(input_value, '[^\d\.]', 'g') v_symbol
    order by
      input_row
  )

  ,c3020 as (
    select
      input_row,
      input_value,
      n_symbol,
      n_symbol_nth,
      regexp_instr(input_value, format('\%s', n_symbol), 1, n_symbol_nth::int) - 1 as n_symbol_pos
    from
      c3010
  )

  ,c3999 as (
    select
      input_row,
      input_value,
      n_symbol,
      n_symbol_pos,
      box2d(st_buffer(st_geomfromewkt(format('LINESTRING(%s %s, %s %s)',
        n_symbol_pos, -input_row,
        n_symbol_pos + length(n_symbol::text), -input_row + 1
      )), 1)) as mask_geom
    from
      c3020
  )

  ,c4010 as (
    select
      input_row,
      input_value,
      number_value,
      number_geom,
      mask_geom,
      mask_area,
      mask_geom is not null as number_matched
    from
      c2999
        left join lateral (
          select
            array_agg(mask_geom) as mask_geom,
            array_agg(st_area(st_intersection(number_geom, mask_geom))) as mask_area
          from
            c3999 m
          where true
            and st_intersects(number_geom, mask_geom)
            and st_area(st_intersection(number_geom, mask_geom)) > 0
        ) m on true
  ),
  c4020 as (
    select
      sum(number_value::int4) filter ( where number_matched ) as numbers_sum
    from
      c4010
  )

select * from c4020;
