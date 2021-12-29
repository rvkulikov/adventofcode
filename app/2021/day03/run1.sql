drop schema if exists day03 cascade;
create schema day03;

create table day03.input (
  sequence_id serial,
  sequence_bits text
);

\copy day03.input (sequence_bits) from '2021/day03/input.csv' with (format 'text');

with
  carinality as (
    select
      length(sequence_bits) as value
    from
      day03.input
    limit
      1
  ),
  matrix as (
    select
      s.digit,
      (regexp_split_to_array(sequence_bits::text, ''))[s.digit] as bit
    from
      day03.input
        left join carinality on true
        cross join generate_series(1, carinality.value) s (digit)
  ),
  columns as (
    select
      digit,
      count(bit) filter ( where bit::bit = 1::bit ) as positives,
      count(bit) filter ( where bit::bit = 0::bit ) as negatives
    from
      matrix
    group by
      digit
    order by
      digit
  )
select
  format('Part 1 answer is: %s',
     lpad(array_to_string(array_agg(case when positives > negatives then 1 else 0 end), '')::text, 32, '0')::bit(32)::int *
     lpad(array_to_string(array_agg(case when positives > negatives then 0 else 1 end), '')::text, 32, '0')::bit(32)::int
  )
from
  columns;