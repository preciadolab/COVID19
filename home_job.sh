#!/bin/bash
#$ -S /bin/bash      # <- use bash shell
#$ -N food_job		 # <- name of job
#$ -cwd              # <- run job 'here' (required!!)
#$ -t 1-67    # <- array job first-last
#$ -o output/output_$TASK_ID.out  # <- output file
#$ -j y              # <- join output & error (best!)

cd /fsx/paco/multiscale_epidemic/food_distribution_sites/
python3 findHome.py -k ${SGE_TASK_ID}
