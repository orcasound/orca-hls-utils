
## Goals

1. Use this `orca-hls-utils` package to fetch and process audio data from the Orcasound archive for a given location and time range.

2. Improve functionality of the existing package in locating and listing the corresponding HLS files for a given location and time range. Design a simple algorithm using S3 primitives based on notes below. Implement in a modular way so that existing classes are unchanged.

3. To test (1) and (2), we will use $LOCATION_NAME=rpi_north_sjc in an example URL like below:
https://s3-us-west-2.amazonaws.com/audio-orcasound-net/$LOCATION_NAME/hls/1755154822/live.m3u8

Time range: 12/29/2025 9pm PST to 10pm PST


## Previous Rough Notes

### Verify archive data access

- [x] collect key resource urls and wikis
    - AWS registry page: https://registry.opendata.aws/orcasound/
        buckets: `streaming-orcasound-net` and `audio-orcasound-net`
    - (likely out of date) orcadata wiki: https://github.com/orcasound/orcadata/wiki/Orcasound-S3-HLS-archives
        had per s3 bucket prefix hierarchy for per node hls archives
    - (seems more recent) orcadata: https://github.com/orcasound/orcadata/blob/master/access.md
        buckets migrated to `audio-orcasound-net` and `audio-deriv-orcasound-net`
        browser: https://open.quiltdata.com/b/audio-orcasound-net/tree/ and https://open.quiltdata.com/b/audio-deriv-orcasound-net/tree/
    - python package `orca-hls-utils`: https://github.com/orcasound/orca-hls-utils
        summary: https://github.com/copilot/c/23257bbc-2ee3-49ef-a3ce-20c612c1824f
- [x] manually try to local hls for one orcahello ai detection response
    - id: 3c889751-6c2b-4ff8-8624-c6b9d1ff17de
      timestamp: 2025-08-14T03:01:25.267833Z
      location: Bush Point

      ```python
      import datetime
      iso_ts = "2025-08-14T03:01:25.267833Z"
      dt = datetime.datetime.fromisoformat(iso_ts.replace('Z', '+00:00'))
      print(dt.timestamp())  # UNIX timestamp in seconds
      # > 1755140485.268
      ```

      output:
        hydrophone: rpi_bush_point
        stream_unix_timestamp: 1755068418
        offset_seconds: 72067
        playlist_index: 

- [x] algorithm idea:
    convert timestamp to unix
        > 1755140485
    use first 4 digits of unix timestamp to prefix-filter HLS bucket objects
        > prefix "1755" https://open.quiltdata.com/b/audio-orcasound-net/tree/rpi_bush_point/hls/?prefix=1755
        > [1755068418, 1755154817, 1755241217, ...]
    get insertion index of unix timestamp in sorted prefix-filtered list
        > index 0
    find base stream and calculate time offset from stream start
        > base stream: 1755068418
        > time offset: 1755140485 - 1755068418 = 72067 seconds


### Archive data access post processing

- eventual goal:
    compare orcahello ai detections with listener network reports https://live.orcasound.net/reports
    allow browsing/listening to orcahello ai daily/weekly detections with similar website experience
        https://live.orcasound.net/bouts/bout_031bSwgJrjYOz8WsU9uBEl
        https://live.orcasound.net/reports/cand_031wqcwzVeSaHRUpENMW11

    download appropriate dataset of orcahello ai api detections aligned with archive HLS files (true/false positives and false negatives)
    download subsampled longitudinal archives (e.g. March to Sep 2024) for model validation

- [ ] align ~8k Orcahello AI api detections with archive HLS files
