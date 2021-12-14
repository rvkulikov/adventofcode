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
  count(*)
from
  data
where true
  and prev < curr;