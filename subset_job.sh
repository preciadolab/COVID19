#!/bin/bash
#$ -S /bin/bash      # <- use bash shell
#$ -N subset_mob     # <- name of job
#$ -cwd              # <- run job 'here' (required!!)
#$ -t 1-67           # <- array job first-last
#$ -o output/output_$TASK_ID.out  # <- output file
#$ -j y              # <- join output & error (best!)

cd /fsx/paco/multiscale_epidemic/subsets_and_joins/
python3 subset_mobility.py -k ${SGE_TASK_ID}
