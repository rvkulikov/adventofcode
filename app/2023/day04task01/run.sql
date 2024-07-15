--@formatter:off
drop schema if exists aoc_2023_day04_task01 cascade;
create schema aoc_2023_day04_task01;

set session search_path to aoc_2023_day04_task01,public;

create table aoc_2023_day04_task01.input (
  input_row_number int4 generated always as identity,
  input_string text
);

\copy aoc_2023_day04_task01.input (input_string) from '2023/day04task01/input.csv' with (format 'text');

with
  c0010 as (
    select
      *
    from
      aoc_2023_day04_task01.input
  ),

  c1010 as (
    select
      (trim(matches[1]))::int4 as card_number,
      (regexp_split_to_array(trim(matches[2]), '\s+'))::int4[] as card_needle_numbers,
      (regexp_split_to_array(trim(matches[3]), '\s+'))::int4[] as card_haystack_numbers
    from
      c0010,
      regexp_match(input_string, 'Card\s+(\d+):([^|]+)\|(.+)') matches
  ),

  c1020 as (
    select
      card_number,
      card_needle_number
    from
      c1010,
      unnest(card_needle_numbers) as card_needle_number
    where true
      and card_needle_number = any(card_haystack_numbers)
  ),

  c1030 as (
    select
      card_number,
      card_win_numbers_count,
      2 ^ (card_win_numbers_count - 1) as card_points
    from
      c1010 c
        inner join lateral (
          select
            count(card_needle_number) as card_win_numbers_count
          from
            c1020 n
          where true
            and c.card_number = n.card_number
        ) n on true
          and card_win_numbers_count > 0
  ),
  c1040 as (
    select
      (sum(card_points))::text as card_points_sum
    from
      c1030
  )

select * from c1040;
