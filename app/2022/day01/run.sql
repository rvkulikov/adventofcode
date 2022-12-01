drop schema if exists day01 cascade;
create schema day01;

create table day01.input (
  input_id serial,
  input_value text
);

\copy day01.input (input_value) from '2022/day01/input.csv' with (format 'text');

with
  cte10 as (
    select
      unnest(regexp_split_to_array(string_agg(day01.input.input_value, ','), ',,', 'i')) as elf_pack
    from
      day01.input
  ),
  cte20 as (
    select
      row_number() over () as elf_id,
      string_to_array(elf_pack, ',') as elf_items
    from
      cte10
  ),
  cte30 as (
    select
      cte20.elf_id as elf_id,
      sum(i.item_calories) as elf_total,
      cte20.elf_items as elf_items
    from
      cte20
        left join lateral (
          select
            unnest(_cte20.elf_items)::numeric as item_calories
          from
            cte20 _cte20
          where
              _cte20.elf_id = cte20.elf_id
        ) i on true
    group by
      1, 3
    having
      sum(i.item_calories) < 90000
    order by
      2 desc
  ),
  ans1 as (
    select
      *
    from
      cte30
    where
      elf_total < 90000
    limit
      1
  ),
  ans2 as (
    select
      sum(t.total)
    from (
      select
        elf_total
      from
        cte30
      limit
        3
    ) t (total)
  )
select
  *
from
  ans2