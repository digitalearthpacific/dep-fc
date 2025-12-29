# Digital Earth Pacific Fractional Cover

This repository implements the Geoscience Australia Fractional Cover
analysis for Digital Earth Pacific. The code here is primarily a wrapper
around [the canonical implementation](https://github.com/GeoscienceAustralia/fc).

## Project Structure

### [src/](src/)

- [process_scenes_for_year.py](src/process_scenes_for_year.py)
  Process all Landsat data for a single pathrow in a year

- [create_annual_summary.py](src/create_annual_summary.py)
  Create annual summaries across all fractional cover data for a year.

- [process_recent_landsat_scenes.py](src/process_recent_landsat_scenes.py)
  Create fractional cover for recent Landsat scenes in a pathrow.

### [workflow/](.workflow/)

Processing at scale was accomplished using [Argo workflows](https://argoproj.github.io/).
This folder contains workflows used to produce all data outputs.

The cron-based workflow to create fractional cover for recent scenes is in the
[DE Pacific WOfS repository](https://github.com/digitalearthpacific/dep-wofs).

- [fc.yaml](workflow/fc.yaml)
  For creating scene-level fractional cover data.

- [fc_summary_annual](workflow/fc_summary_annual.yaml)
  For creating annual summaries.

## Installation

Please refer to the [Dockerfile](Dockerfile) for prerequisites for installation.
