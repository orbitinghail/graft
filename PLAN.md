- build a simple GC that simply drops orphan segments
  - pay special attention to in-progress VolumeWriters
  - make sure to run fjall gc when deleting pages
- consider adding a read oracle (do some perf testing)
- create some hello-world examples of using Graft without SQLite
- port tests
- write first draft of antithesis tests

# taxonomy v2

High level:

- rename volume* to log*
- rename graft to volume

Key properties:

- Volumes are local only concepts
- Logs are remote and local concepts
- A volume tracks a local and remote log, and serves as a logical view
- A volume's local log only contains outstanding local change
- A volume is keyed by it's local log id
- A tag references a volume by name
