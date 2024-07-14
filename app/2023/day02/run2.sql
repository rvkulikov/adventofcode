--@formatter:off
drop schema if exists aoc_2023_day02_run2 cascade;
create schema aoc_2023_day02_run2;

set session search_path to aoc_2023_day02_run2,public;

create table aoc_2023_day02_run2.input (
  input_id int4 generated always as identity ,
  input_value text
);

create aggregate aoc_2023_day02_run2.product(int4) (
  sfunc = int4mul,
  stype=int4
);

\copy aoc_2023_day02_run2.input (input_value) from '2023/day02/input.csv' with (format 'text');

with
  c1010 as (
    select
      input_parts[1] as game_log,
      (regexp_match(input_parts[1], 'game (\d+)', 'i'))[1]::int4 as game_id,

      input_parts[2] as game_session_log
    from
      aoc_2023_day02_run2.input,
      string_to_array(input_value, ':') input_parts
  ),

  c1015 as (
    select
      *
    from
      c1010, regexp_split_to_table(game_session_log, ';') s (session_log)
  ),
  c1020 as (
    select
      *,
      (regexp_match(l_roll_log, '(\d+)\s+\D+', 'i'))[1]::int4 as roll_number,
      (regexp_match(l_roll_log, '\d+\s+(\D+)', 'i'))[1]::text as roll_color
    from
      c1015,
      regexp_split_to_table(session_log, ',') s (l_roll_log)
  ),
  c1030 as (
    select
      game_id,
      roll_number,
      roll_color
    from
      c1020
  ),

  c2010 as (
    select
      game_id,
      game_session_log,
      array_agg(roll_color_max_number) as game_color_max_numbers,
      product(roll_color_max_number) as game_power
    from
      c1010 g
        inner join lateral (
          select
            r.roll_color,
            max(r.roll_number) as roll_color_max_number
          from
            c1030 r
          where true
            and r.game_id = g.game_id
          group by
            1
        ) p on true
    group by
      1, 2
  ),
  c2020 as (
    select
      sum(game_power) as games_sum
    from
      c2010
  )

select * from c2020;
