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
  interarrivals_cs_by_source = []
  interarrivals_sc_by_source = []

  interarrivals_cs_by_source_target = []
  interarrivals_sc_by_source_target = []

  timedelta_zero = timedelta(0)
  timedelta_1usec = timedelta(microseconds=1)

  fname_CS_S = 'interarrivals_CS_S.raw'
  fname_SC_S = 'interarrivals_SC_S.raw'
  fname_CS_ST = 'interarrivals_CS_ST.raw'
  fname_SC_ST = 'interarrivals_SC_ST.raw'

  dname_by_source_id = 'grouped_by_source_id'
  dname_by_source_target_id = 'grouped_by_source_target_id'

  for dname in [dname_by_source_id, dname_by_source_target_id]:
    try:
      os.mkdir(dname)
    except FileExistsError:
      pass

  if FLAGS.mode == 'group':
    for dname in ['05', '06']:
      for fname in os.listdir(dname):
        p = os.path.join(dname, fname)
        print(f'* Processing {p}')
        with open(p) as infile:
          count = 0
          for line in infile:
            count += 1
            fields = line.strip().split(' ')
            ts = None
            direction = None
            length = 0

            ts_string = fields[1]
            if ts_string.endswith('Z'):
              ts_string = ts_string.strip('Z')

            ts = datetime.fromisoformat(ts_string)
            source_id = fields[3]
            target_id = fields[4]

            processing_time = timedelta(milliseconds=0)
            request_processing_time = timedelta(milliseconds=float(fields[5]))  # ELB -> target
            if request_processing_time > timedelta_zero:
              processing_time += request_processing_time

            target_processing_time = timedelta(milliseconds=float(fields[6]))  # by target
            if target_processing_time > timedelta_zero:
              processing_time += target_processing_time

            response_processing_time = timedelta(milliseconds=float(fields[7]))  # target -> ELB
            if response_processing_time > timedelta_zero:
              processing_time += response_processing_time

            with open_after_dir_creation(os.path.join(dname_by_source_id, compute_hashed_file_path(f'{source_id}_CS')), 'a') as outfile:
              print(ts.isoformat(), file=outfile)

            with open_after_dir_creation(os.path.join(dname_by_source_id, compute_hashed_file_path(f'{source_id}_SC')), 'a') as outfile:
              print((ts + processing_time).isoformat(), file=outfile)

            with open_after_dir_creation(os.path.join(dname_by_source_target_id, compute_hashed_file_path(f'{source_id}_{target_id}_CS')), 'a') as outfile:
              print(ts.isoformat(), file=outfile)

            with open_after_dir_creation(os.path.join(dname_by_source_target_id, compute_hashed_file_path(f'{source_id}_{target_id}_SC')), 'a') as outfile:
              print((ts + processing_time).isoformat(), file=outfile)

  if FLAGS.mode in ['group', 'interarrival']:
    for e in [
      (dname_by_source_id, (fname_CS_S, fname_SC_S)),
      (dname_by_source_target_id, (fname_CS_ST, fname_SC_ST)),
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

            with open(fname) as infile:
              tss = []
              for line in infile:
                ts = datetime.fromisostring(line)
                tss.append(ts)
              tss = sorted(tss)
              for i in range(0, len(tss) - 1):
                interarrival = tss[i + 1] - tss[i]
                if interarrival < timedelta_zero:
                  print('except', fname, count, source_id, tss[i + 1], tss[i], interarrival)

                interarrival_msec = interarrival / timedelta(microseconds=1) / 1000.0
                print(interarrival_msec, file=outfile)


  if FLAGS.mode in ['group', 'interarrival', 'gnuplot']:
    for e in [
        ('CS', 'S', fname_CS_S), ('SC', 'S', fname_SC_S),
        ('CS', 'ST', fname_CS_ST), ('SC', 'ST', fname_SC_ST),
    ]:
      suffix, group, fname = e

      interarrivals_sum = 0
      interarrivals_sum2 = 0
      interarrivals_msecs = numpy.loadtxt(fname)
      with open(f'interarrivals_{suffix}_{group}.data', 'w') as f:
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
        with open(f'interarrivals_{suffix}_{group}.stats', 'w') as f2:
          print('total_count', total_count, file=f2)
          print('length_sum', interarrivals_sum, file=f2)
          print('length_sum2', interarrivals_sum2, file=f2)
          print('Average', avg, file=f2)
          print('var', var, file=f2)
          print('Stdev', math.sqrt(var), file=f2)

      with open(f'interarrivals_{suffix}_{group}.gnuplot', 'w') as f:
        commands = f'''
set term pdf enhanced color font "Arial, 12"
set output "interarrivals_{suffix}_{group}.pdf"
set xlabel "Packet Interarrival Time in Milliseconds"
set ylabel "Fractions of #Packets"
set xtics auto
set ytics auto
set logscale x 10
plot "interarrivals_{suffix}.data" using 1:2 title "Cumulative" with lines
'''
        print(commands, file=f)

      subprocess.run(['gnuplot', f'interarrivals_{suffix}_{group}.gnuplot'], encoding='utf-8')


if __name__ == '__main__':
  app.run(main)
