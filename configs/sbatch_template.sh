#!/usr/bin/env bash
#SBATCH --job-name=dolma_pipeline
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=12:00:00
#SBATCH --output=jobs/%x_%j.out

module load python/3.10 || true
source dolma/env/setup.sh

python dolma/scripts/pipeline_runner.py --stage both
