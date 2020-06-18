#!/bin/bash
#$ -S /bin/bash      # <- use bash shell
#$ -N visits_job		 # <- name of job
#$ -cwd              # <- run job 'here' (required!!)
#$ -t 16-25,46-55    # <- array job first-last
#$ -o output/output_$TASK_ID.out  # <- output file
#$ -j y              # <- join output & error (best!)

cd /fsx/paco/multiscale_epidemic/food_distribution_sites/
python3 findVisits.py -k ${SGE_TASK_ID}