drop schema if exists day02 cascade;
create schema day02;

create table day02.input (
  movement_id serial,
  movement_direction text,
  movement_distance int
);

\copy day02.input (movement_direction, movement_distance) from '2021/day02/input.csv' with (delimiter ' ');

with
  deltas as (
    select
      case
        when movement_direction = 'forward' then movement_distance
        else 0
      end as horizontal,
      case
        when movement_direction = 'up' then -movement_distance
        when movement_direction = 'down' then movement_distance
        else 0
      end as vertical
    from
      day02.input
  )
select
  format('Part 1 answer is: %s', sum(horizontal) * sum(vertical))
from
  deltas;

with
  deltas as (
    select
      case
        when movement_direction = 'forward' then movement_distance
        else 0
      end as horizontal,
      case
        when movement_direction = 'forward' then movement_distance * a.aim_sum
        else 0
      end as vertical
    from
      day02.input i
        cross join lateral (
          select
            sum(case
              when movement_direction = 'up' then -movement_distance
              when movement_direction = 'down' then movement_distance
              else 0
            end) as aim_sum
          from
            day02.input _i
          where
            _i.movement_id <= i.movement_id
        ) a
    order by
      movement_id
  )
select
  format('Part 2 answer is: %s', sum(horizontal) * sum(vertical))
from
  deltas