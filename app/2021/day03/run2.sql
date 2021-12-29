--@formatter:off
drop schema if exists day03 cascade;
create schema day03;

create table day03.input (
  sequence_id serial,
  sequence_bits text
);

\copy day03.input (sequence_bits) from '2021/day03/input.csv' with (format 'text');

with
  matrices as (
    select
      array_agg(regexp_split_to_array(sequence_bits::text, '')) as matrix,
      to_jsonb(array_agg(regexp_split_to_array(sequence_bits::text, ''))) as data
    from
      day03.input
  ),
  transpose as (
    select
      array(
        select (
          array (
            select matrix[j][k]
            from ( select generate_subscripts(matrix, 1) as j ) foo
          )
        )
        from ( select generate_subscripts(matrix, 2) as k ) bar
      ) as matrix,
      to_jsonb(array(
        select (
                 array (
                   select matrix[j][k]
                   from ( select generate_subscripts(matrix, 1) as j ) foo
                   )
                 )
        from ( select generate_subscripts(matrix, 2) as k ) bar
        )) as data

    from matrices m
  )
--   metrics as (
    select
  i,
  matrix[i:i],
  s.negatives,
  s.positives,
      array_to_string(array_agg(case
        when s.negatives > s.positives then 0
        when s.negatives = s.positives then 1
        else 1
      end), '') as generator_bin,
      array_to_string(array_agg(case
        when s.negatives > s.positives then 1
        else 0
      end), '') as scrubber_bin
    from
      transpose
        cross join generate_subscripts(matrix, 1) with ordinality as i
        cross join lateral (
          select
            count(*) filter ( where bit = 0 ) as negatives,
            count(*) filter ( where bit = 1 ) as positives
          from
            unnest(matrix[i:i]::int[]) as t (bit)
          where
            -- отфильтровать по битовой маске
        ) s
group by
  1, 2, 3, 4
--   )
-- select
--   *,
--   lpad(generator_bin::text, 32, '0')::bit(32)::int,
--   lpad(scrubber_bin::text, 32, '0')::bit(32)::int,
--   format('Part 2 answer is: %s',
--     lpad(generator_bin::text, 32, '0')::bit(32)::int *
--     lpad(scrubber_bin::text, 32, '0')::bit(32)::int
--   )
-- from
--   metrics

-- резвернуть winner и найти строки которые равные виннеру и посчитать
-- инверснуть виннера через xor и посчитать