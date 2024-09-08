#!/usr/bin/bash

# Video games only
# poetry run python -m danker -i danker_links_video_games.tsv 0.85 30 1 > danker_video_games.output

# All
poetry run python -m danker -i danker_links_all.tsv 0.85 30 1 > danker_all.output

