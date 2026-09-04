window.BENCHMARK_DATA = {
  "lastUpdate": 1788549244306,
  "repoUrl": "https://github.com/FalkAurel/sqlengine",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "137809006+FalkAurel@users.noreply.github.com",
            "name": "Falk Aurel",
            "username": "FalkAurel"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f400add01e6a7170930cc3bba538326b42f38377",
          "message": "Merge pull request #4 from FalkAurel/feature/benchmark_integration\n\nDisable bench on lib and bin targets so --benches only runs explicit …",
          "timestamp": "2026-09-04T21:06:35+02:00",
          "tree_id": "9bb340493cbeb250b322c1c9bd3c98167423cd64",
          "url": "https://github.com/FalkAurel/sqlengine/commit/f400add01e6a7170930cc3bba538326b42f38377"
        },
        "date": 1788549241974,
        "tool": "cargo",
        "benches": [
          {
            "name": "column_write/1024",
            "value": 1810,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "column_write/16384",
            "value": 27612,
            "range": "± 212",
            "unit": "ns/iter"
          },
          {
            "name": "column_write/65536",
            "value": 104366,
            "range": "± 637",
            "unit": "ns/iter"
          },
          {
            "name": "column_write/131072",
            "value": 208417,
            "range": "± 1147",
            "unit": "ns/iter"
          },
          {
            "name": "column_write/1048576",
            "value": 2760586,
            "range": "± 12616",
            "unit": "ns/iter"
          },
          {
            "name": "chunk_writer_append/65536",
            "value": 88073,
            "range": "± 3166",
            "unit": "ns/iter"
          },
          {
            "name": "column_write_values/1024",
            "value": 142,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "column_write_values/16384",
            "value": 1831,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "column_write_values/65536",
            "value": 7082,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "column_write_values/131072",
            "value": 13611,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "column_write_values/1048576",
            "value": 305990,
            "range": "± 539",
            "unit": "ns/iter"
          },
          {
            "name": "column_creation",
            "value": 79,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_append/1024",
            "value": 1477,
            "range": "± 161",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_append/16384",
            "value": 30957,
            "range": "± 1646",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_append/65536",
            "value": 116494,
            "range": "± 8239",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_append/131072",
            "value": 228431,
            "range": "± 16397",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_append/1048576",
            "value": 1701217,
            "range": "± 143835",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_chunked_retained/1024",
            "value": 1552,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_chunked_retained/16384",
            "value": 28931,
            "range": "± 554",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_chunked_retained/65536",
            "value": 111334,
            "range": "± 2231",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_chunked_retained/131072",
            "value": 223382,
            "range": "± 4228",
            "unit": "ns/iter"
          },
          {
            "name": "raw_arrow_chunked_retained/1048576",
            "value": 1816197,
            "range": "± 18598",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}