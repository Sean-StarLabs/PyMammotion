# Mammotion HA fork wheel

The HA fork wheel is built from a clean tracked `HEAD` archive. It does not
modify the SDK source tree. The builder accepts only the official PyPI
`pymammotion` 0.8.14 wheel with the recorded SHA-256, checks its metadata and
credential sentinel block, and copies that block into a temporary archive of
the fork source. Every other byte of `pymammotion/const.py` must match outside
the sentinel block. The wheel version is `0.8.14.post1`.

With Python 3.14 and Hatchling 1.32.4 available locally, build offline with:

```sh
python scripts/build_fork_wheel.py \
  --official-wheel /path/to/pymammotion-0.8.14-py3-none-any.whl \
  --sha256 18a9d160388c678a0221555b857221ab08a5e875af0d66ee030581fa3a1629a9 \
  --output /private/artifacts/pymammotion-0.8.14.post1-py3-none-any.whl
```

The input hash is the official PyPI release wheel hash. Keep the output
directory private; the resulting wheel contains the same release-injected
application configuration as that official wheel. The builder sets
`SOURCE_DATE_EPOCH` from the source commit and writes the artifact with mode
`0600`.
