#!/usr/bin/python3

from absl import app, flags
from datetime import datetime, timedelta

import math
import numpy
import os
import subprocess
import sys


FLAGS = flags.FLAGS

flags.DEFINE_enum('mode', 'gnuplot', ['group', 'interarrival', 'gnuplot'], 'Whether to group by sesssion id')


def compute_hashed_file_path(name):
  dname = os.path.join(name[0:2], name[2:4])
  return os.path.join(dname, name)


def open_after_dir_creation(p, mode):
  dname, fname = os.path.split(p)
  try:
    os.makedirs(dname)
  except FileExistsError:
    pass
  return open(p, mode)


def main(argv):
  per_session_ids_cs = {}
  per_session_ids_sc = {}

  interarrivals_cs = []
  interarrivals_sc = []

  fname_CS = 'interarrivals_CS.raw'
  fname_SC = 'interarrivals_SC.raw'

  dname_by_session_id = 'grouped_by_session_id'

  if FLAGS.mode == 'group':
    for line in sys.stdin:
      fields = line.strip().split(' ')
      ts = None
      direction = None
      session_id = None

      try:
        ts = datetime.strptime(fields[1], '%H:%M:%S.%f')
        direction = fields[4]
        if direction not in ['[C->S]', '[S->C]']:
          continue
        session_id = fields[8].split('=')[-1]
      except Exception as e:
        print(f'{e}: Invalid line: {line}')
        continue

      suffix = 'CS' if direction == '[C->S]' else 'SC' if direction == '[S->C]' else None
      if not suffix:
        print('Skipping', line)
        continue

      with open_after_dir_creation(os.path.join(dname_by_session_id, compute_hashed_file_path(f'{session_id}_{suffix}')), 'a') as outfile:
        print(ts.isoformat(), file=outfile)

  if FLAGS.mode in ['group', 'interarrival']:
    timedelta_zero = timedelta(0)
    for e in [
      (dname_by_session_id, (fname_CS, fname_SC)),
    ]:
      dname, (fname_CS, fname_SC) = e
      with (
        open(fname_CS, 'w') as fname_CS_f,
        open(fname_SC, 'w') as fname_SC_f,
      ):
        for root, dirs, files in os.walk(dname):
          for fname in files:
            print(f'Loading {fname}')
            outfile = fname_CS_f if fname.endswith('_CS') else fname_SC_f if fname.endswith('_SC') else None
            if not outfile:
              print(f'Skipping {fname}')
              continue

            with open(os.path.join(root, fname)) as infile:
              tss = []
              for line in infile:
                ts = datetime.fromisoformat(line.strip())
                tss.append(ts)
              tss = sorted(tss)
              for i in range(0, len(tss) - 1):
                interarrival = tss[i + 1] - tss[i]
                if interarrival < timedelta_zero:
                  print('except', fname, count, session_id, tss[i + 1], tss[i], interarrival)

                interarrival_msec = interarrival / timedelta(microseconds=1) / 1000.0
                print(interarrival_msec, file=outfile)

  if FLAGS.mode in ['group', 'interarrival', 'gnuplot']:
    for e in [
        ('CS', fname_CS), ('SC', fname_SC),
    ]:
      suffix, fname = e

      interarrivals_sum = 0
      interarrivals_sum2 = 0
      total_count = 0
      interarrivals_msecs = numpy.loadtxt(fname)
      with open(f'interarrivals_{suffix}.data', 'w') as f:
        total_count = len(interarrivals_msecs)
        cumulative_count = 0
        for interarrival_msec in sorted(interarrivals_msecs):
          cumulative_count += 1
          cumulative_count_f = cumulative_count * 1.0 / total_count
          interarrivals_sum += interarrival_msec
          interarrivals_sum2 += (interarrival_msec * interarrival_msec)
          print(interarrival_msec, cumulative_count_f, file=f)

        avg = (interarrivals_sum * 1.0 / total_count)
        var = (interarrivals_sum2 * 1.0 / total_count) - avg * avg
        with open(f'interarrivals_{suffix}.stats', 'w') as f2:
          print('total_count', total_count, file=f2)
          print('length_sum', interarrivals_sum, file=f2)
          print('length_sum2', interarrivals_sum2, file=f2)
          print('Average', avg, file=f2)
          print('var', var, file=f2)
          print('Stdev', math.sqrt(var), file=f2)

      with open(f'interarrivals_{suffix}.gnuplot', 'w') as f:
        commands = f'''
set term pdf enhanced color font "Arial, 14"
set output "interarrivals_{suffix}.pdf"
set xlabel "Packet Interarrival Time in Milliseconds"
set ylabel "Fractions of #Packets"
set xtics auto
set ytics auto
set key right center
set logscale x 10
plot "interarrivals_{suffix}.data" using 1:2 title "Cumulative" with lines
'''
        print(commands, file=f)

      subprocess.run(['gnuplot', f'interarrivals_{suffix}.gnuplot'], encoding='utf-8')


if __name__ == '__main__':
  app.run(main)
