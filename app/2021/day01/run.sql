drop schema if exists day01 cascade;
create schema day01;

create table day01.input (
  input_id serial,
  input_value int
);

\copy day01.input (input_value) from '2021/day01/input.csv' with (format 'text');

with data as (
  select
    lag(input_value) over w as prev,
    input_value as curr
  from
    day01.input
  window
    w as (
      order by input_id
    )
)
select
  format('Part 1 answer is: %s', count(*))
from
  data
where true
  and prev < curr;

with
  data0 as (
    select
      i.input_value,
      g.group_ids
    from
      day01.input i
        cross join lateral (
          select
            array_agg(_i.input_id) as group_ids
          from
            day01.input _i
          where true
            and _i.input_id > i.input_id - 3
            and _i.input_id <= i.input_id
        ) g
  ),
  data1 as (
    select distinct
      unnest(group_ids) as group_id
    from
      data0
    order by
      group_id
  ),
  data2 as (
    select
      data1.group_id as group_id,
      s.group_sum as group_sum
    from
      data1
        cross join lateral (
          select
            sum(input_value) as group_sum
          from
            data0
          where true
            and group_id = any(group_ids)
        ) s
  ),
  data3 as (
    select
      lag(group_sum) over w as prev,
      group_sum as curr
    from
      data2
    window
      w as (
        order by group_id
      )
  )
select
  format('Part 2 answer is: %s', count(*))
from
  data3
where true
  and prev < curr;