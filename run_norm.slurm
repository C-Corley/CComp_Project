#!/bin/bash

#SBATCH --job-name=multithread
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --partition=gpu-a100 # Name of the partition
#SBATCH --mem=10G # Amount of CPU memory requird per node
#SBATCH --account=class-dsci2022 # Account number
#SBATCH --gres=gpu:a100_1g.10gb:1 # Requests one Nvidia A100 GPU with 10 GB of
#SBATCH --time=1:00:00 # Maximum wall time
#SBATCH --output=output-%j.out # Output file (stdout), %j is job allocation number
#SBATCH --error=err-%j.out # Error file (stderr), %j is job allocation number
#SBATCH --mail-type=ALL # All types all emails (BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=cc3886@msstate.edu # Where to send the email

# Display SLURM job information
echo "Running on $(hostname)"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "SLURM Task ID: $SLURM_ARRAY_TASK_ID"
echo "SLURM Node List: $SLURM_NODELIST"
echo "SLURM CPUS on Node: $SLURM_CPUS_ON_NODE"

# Run the Python script

source /apps/spack-managed/gcc-11.3.1/miniconda3-*/etc/profile.d/conda.sh
conda activate importsClass

srun python3 /scratch/ptolemy/users/cc3886/project_cc/project_test/combine_files/normalize.py
