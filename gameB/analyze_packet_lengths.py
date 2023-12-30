#!/usr/bin/python3

import math
import os
import subprocess
import sys


def main(argv):
  per_lengths_cs = {}
  per_lengths_sc = {}
  per_lengths_all = {}

  lengths_sum = 0
  lengths_sum2 = 0

  for dname in ['05', '06']:
    for fname in os.listdir(dname):
      p = os.path.join(dname, fname)
      print(f'* Processing {p}')
      with open(p) as infile:
        for line in infile:
          fields = line.strip().split(' ')
          ts = None
          direction = None
          length = 0

          ts = fields[1]
          req_length = int(fields[10])
          repl_length = int(fields[11])

          existing = per_lengths_cs.setdefault(req_length, 0)
          per_lengths_cs[req_length] = existing + 1
          existing = per_lengths_all.setdefault(req_length, 0)
          per_lengths_all[req_length] = existing + 1

          existing = per_lengths_sc.setdefault(repl_length, 0)
          per_lengths_sc[repl_length] = existing + 1
          existing = per_lengths_all.setdefault(repl_length, 0)
          per_lengths_all[repl_length] = existing + 1

  for e in [('CS', per_lengths_cs), ('SC', per_lengths_sc), ('ALL', per_lengths_all)]:
    suffix, per_lengths = e

    total_count = 0
    for length in sorted(per_lengths.keys()):
      count = per_lengths[length]
      total_count += count

    lengths_sum = 0
    lengths_sum2 = 0
    cumulative_count = 0
    with open(f'per_lengths_{suffix}.data', 'w') as f:
      for length in sorted(per_lengths.keys()):
        count = per_lengths[length]
        count_f = count * 1.0 / total_count
        cumulative_count += count
        cumulative_count_f = cumulative_count * 1.0 / total_count
        lengths_sum += (length * count)
        lengths_sum2 += (length * length) * count
        print(length, count_f, cumulative_count_f, count, cumulative_count, file=f)

      avg = (lengths_sum * 1.0 / total_count)
      var = (lengths_sum2 * 1.0 / total_count) - avg * avg
      with open(f'per_lengths_{suffix}.stats', 'w') as f2:
        print('total_count', total_count, file=f2)
        print('length_sum', lengths_sum, file=f2)
        print('length_sum2', lengths_sum2, file=f2)
        print('Average', avg, file=f2)
        print('var', var, file=f2)
        print('Stdev', math.sqrt(var), file=f2)

    with open(f'per_lengths_{suffix}.gnuplot', 'w') as f:
      commands = f'''
set term pdf enhanced color font "Arial, 12"
set output "per_lengths_{suffix}.pdf"
set xlabel "Packet Lengths in Bytes"
set ylabel "Fractions of #Packets"
set xtics auto
set ytics auto
set logscale x 10
plot "per_lengths_{suffix}.data" using 1:2 title "Count" with lines, \
     "per_lengths_{suffix}.data" using 1:3 title "Cumulative" with lines
'''
      print(commands, file=f)

    subprocess.run(['gnuplot', f'per_lengths_{suffix}.gnuplot'], encoding='utf-8')


if __name__ == '__main__':
  main(sys.argv)
