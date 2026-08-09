# Syncoid integration fixture

`syncoid-2.3.0` is the exact `syncoid` file from the checksum-pinned
Syncoid 2.3.0 archive used by the release image:

https://github.com/jimsalterjrs/sanoid/archive/refs/tags/v2.3.0.tar.gz

The archive SHA-256 is
`1d8735a271a34ec87ea46313a66f6f20bd38b583886924574d3c1f72ea173620`.
The fixture is kept so the Sender regression remains deterministic and does
not require network access; the release image still downloads and verifies the
archive before applying the repository-owned patch.

`Capture/Tiny.pm` is a minimal test-only shim for the three capture APIs used
by this fixture. The release image continues to use the packaged
`libcapture-tiny-perl` dependency.
